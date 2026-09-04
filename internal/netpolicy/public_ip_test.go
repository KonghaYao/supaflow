package netpolicy

import (
	"net"
	"testing"
)

// 验证共享运营商、基准测试、文档、私网和 IPv6 特殊地址均不能作为公网出口。
func TestIsPublicIPRejectsSpecialUseRanges(t *testing.T) {
	for _, value := range []string{
		"10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.1.1", "192.0.2.1",
		"198.18.0.1", "198.51.100.1", "203.0.113.1", "240.0.0.1", "::1", "2001:db8::1", "fc00::1", "fe80::1",
	} {
		if IsPublicIP(net.ParseIP(value)) {
			t.Fatalf("special-use address %s was allowed", value)
		}
	}
	if !IsPublicIP(net.ParseIP("93.184.216.34")) || !IsPublicIP(net.ParseIP("2606:4700:4700::1111")) {
		t.Fatal("globally routable address was rejected")
	}
}
