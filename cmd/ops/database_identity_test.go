package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// 攻击复现：参数顺序、host alias 或编码不同的等价 DSN 仍解析到同一数据库时必须拒绝。
func TestVerifySeparateRestoreDatabaseRejectsEquivalentDSNs(t *testing.T) {
	productionURL := "postgres://user:secret@db/supaflow?application_name=ops"
	scratchURL := "postgres://user:secret@db-alias/supaflow?application_name=restore"
	identity := databaseIdentity{SystemIdentifier: "7500123456789", DatabaseOID: 16384, DatabaseName: "supaflow"}
	reader := func(_ context.Context, _ string) (databaseIdentity, error) { return identity, nil }
	if err := verifySeparateRestoreDatabase(context.Background(), productionURL, scratchURL, reader); err == nil ||
		!strings.Contains(err.Error(), "separate scratch database") {
		t.Fatalf("equivalent DSNs must fail closed: %v", err)
	}
}

// 真实 PostgreSQL 必须向普通 ops 连接提供 cluster system ID、database OID 和名称。
func TestReadPostgresDatabaseIdentity(t *testing.T) {
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	identity, err := readPostgresDatabaseIdentity(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if identity.SystemIdentifier == "" || identity.DatabaseOID == 0 || identity.DatabaseName == "" {
		t.Fatalf("incomplete database identity: %#v", identity)
	}
}

// 同一 PostgreSQL cluster 上 OID 和名称不同的 scratch database 应被识别为独立目标。
func TestVerifySeparateRestoreDatabaseAllowsDistinctDatabase(t *testing.T) {
	reader := func(_ context.Context, databaseURL string) (databaseIdentity, error) {
		if databaseURL == "production" {
			return databaseIdentity{SystemIdentifier: "7500123456789", DatabaseOID: 16384, DatabaseName: "supaflow"}, nil
		}
		return databaseIdentity{SystemIdentifier: "7500123456789", DatabaseOID: 16385, DatabaseName: "supaflow_restore"}, nil
	}
	if err := verifySeparateRestoreDatabase(context.Background(), "production", "scratch", reader); err != nil {
		t.Fatal(err)
	}
}

// 攻击复现：preflight 后连接被换回 production 时，同一 restore session 的首条身份 guard 必须阻止任何归档 SQL。
func TestRunVerifiedRestoreBindsGuardToVerifiedScratchIdentity(t *testing.T) {
	calls := 0
	restored := false
	reader := func(_ context.Context, databaseURL string) (databaseIdentity, error) {
		calls++
		production := databaseIdentity{SystemIdentifier: "7500123456789", DatabaseOID: 16384, DatabaseName: "supaflow"}
		if databaseURL == "production" {
			return production, nil
		}
		return databaseIdentity{SystemIdentifier: "7500123456789", DatabaseOID: 16385, DatabaseName: "restore"}, nil
	}
	err := runVerifiedRestore(context.Background(), "production", "scratch", reader, func(production, expectedScratch databaseIdentity) error {
		// This models the identity observed by the third connection after a DNS or
		// proxy switch. Production RunRestore performs this comparison in SQL
		// before the archive, on that exact connection and transaction.
		actualRestoreConnection := production
		if actualRestoreConnection != expectedScratch {
			return errors.New("restore target identity does not match verified scratch database")
		}
		restored = true
		return nil
	})
	if err == nil || restored || calls != 2 {
		t.Fatalf("err=%v restored=%v calls=%d", err, restored, calls)
	}
}
