//go:build linux

package isolation

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

// 验证 Linux hard rlimit 在独立进程中全部生效，避免不可逆地污染测试进程。
func TestApplyResourceLimitsInSubprocess(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=TestResourceLimitHelperProcess")
	command.Env = append(os.Environ(), "SUPAFLOW_RESOURCE_LIMIT_HELPER=1")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resource limit helper failed: %v", err)
	}
	var got [5]uint64
	if err = json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode helper output %q: %v", output, err)
	}
	// RLIMIT_NPROC is intentionally unchanged: it is a real-UID-wide counter,
	// not an invocation boundary. Per-attempt process limits are cgroup-backed.
	want := [5]uint64{29, 1 << 40, 48, 1 << 20, 0}
	if got != want {
		t.Fatalf("hard limits = %v, want %v", got, want)
	}
}

// 验证 setrlimit helper 只在父测试创建的子进程中运行。
func TestResourceLimitHelperProcess(t *testing.T) {
	if os.Getenv("SUPAFLOW_RESOURCE_LIMIT_HELPER") != "1" {
		return
	}
	limits := resolvedLimits{cpuSeconds: 29, addressSpaceBytes: 1 << 40, processes: 31, openFiles: 48, fileSizeBytes: 1 << 20}
	if err := applyResourceLimits(limits); err != nil {
		os.Exit(2)
	}
	resources := [...]int{unix.RLIMIT_CPU, unix.RLIMIT_AS, unix.RLIMIT_NOFILE, unix.RLIMIT_FSIZE}
	var values [5]uint64
	for index, resource := range resources {
		var limit unix.Rlimit
		if err := unix.Getrlimit(resource, &limit); err != nil {
			os.Exit(3)
		}
		values[index] = limit.Max
	}
	if err := json.NewEncoder(os.Stdout).Encode(values); err != nil {
		os.Exit(4)
	}
	os.Exit(0)
}
