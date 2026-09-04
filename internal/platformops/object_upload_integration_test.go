package platformops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/postgres"
)

func objectRecoveryFixture(t *testing.T) (context.Context, *pgxpool.Pool, *FileObjectStore, string, string) {
	t.Helper()
	databaseURL := os.Getenv("SUPAFLOW_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SUPAFLOW_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("object_recovery_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
		admin.Close()
	})
	migrations, err := postgres.LoadMigrations(os.DirFS("../.."), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if err = postgres.NewMigrator(pool).Up(ctx, migrations); err != nil {
		t.Fatal(err)
	}
	tenantA := "upload_a"
	tenantB := "upload_b"
	if _, err = pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$1),($2,$2)`, tenantA, tenantB); err != nil {
		t.Fatal(err)
	}
	objects, err := NewFileObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return ctx, pool, objects, tenantA, tenantB
}

func beginTestUpload(t *testing.T, ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, tenantID, id string) *objectUpload {
	t.Helper()
	upload, err := beginObjectUpload(ctx, pool, objects, objectUploadSpec{
		tenantID: tenantID, id: id, kind: "output", key: id, contentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	return upload
}

// 验证 SIGKILL 等价的已发布 orphan 只能由租户内一个恢复者清理，且不会触碰其他租户。
func TestObjectUploadRecoveryIsConcurrentAndTenantScoped(t *testing.T) {
	ctx, pool, objects, tenantA, tenantB := objectRecoveryFixture(t)
	uploadA := beginTestUpload(t, ctx, pool, objects, tenantA, "object_a")
	if _, err := uploadA.put(ctx, bytes.NewReader([]byte("abandoned-a")), 1024); err != nil {
		t.Fatal(err)
	}
	publishTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = uploadA.publishTx(ctx, publishTx); err != nil {
		t.Fatal(err)
	}
	// A killed process rolls back the database transaction, while the atomic
	// filesystem rename remains durable and must be reconciled from the intent.
	if err = publishTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := uploadA.closeHeartbeat(); err != nil {
		t.Fatal(err)
	}
	uploadB := beginTestUpload(t, ctx, pool, objects, tenantB, "object_b")
	if _, err := uploadB.put(ctx, bytes.NewReader([]byte("abandoned-b")), 1024); err != nil {
		t.Fatal(err)
	}
	if err := uploadB.closeHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE object_upload_intents
		SET lease_expires_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	retention, _ := NewRetention(pool, objects, 1024)
	results := make(chan int, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			count, err := retention.RecoverObjects(ctx, tenantA, 10)
			results <- count
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	total := 0
	for count := range results {
		total += count
	}
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 1 {
		t.Fatalf("recovered=%d want=1", total)
	}
	pathA, _ := objects.path(tenantA, "object_a")
	pathB, _ := objects.path(tenantB, "object_b")
	if _, err := os.Stat(pathA); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tenant A published orphan still exists: %v", err)
	}
	if _, err := os.Stat(pathB + ".upload." + uploadB.ownerToken); err != nil {
		t.Fatalf("tenant B stage was touched: %v", err)
	}
	var tenantBIntents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM object_upload_intents WHERE tenant_id=$1`, tenantB).Scan(&tenantBIntents); err != nil || tenantBIntents != 1 {
		t.Fatalf("tenant B intents=%d err=%v", tenantBIntents, err)
	}
	if err := uploadB.abort(); err != nil {
		t.Fatal(err)
	}
}

// 验证 lease takeover 后旧 owner 无法发布，也无法删除新 owner 已提交的 final 对象。
func TestStaleObjectUploadOwnerCannotPublishOrAbortNewOwner(t *testing.T) {
	ctx, pool, objects, tenantA, _ := objectRecoveryFixture(t)
	oldUpload := beginTestUpload(t, ctx, pool, objects, tenantA, "object_race")
	if _, err := oldUpload.put(ctx, bytes.NewReader([]byte("old")), 1024); err != nil {
		t.Fatal(err)
	}
	_ = oldUpload.closeHeartbeat()
	if _, err := pool.Exec(ctx, `UPDATE object_upload_intents SET lease_expires_at=now()-interval '1 second'
		WHERE tenant_id=$1 AND id=$2`, tenantA, oldUpload.id); err != nil {
		t.Fatal(err)
	}
	retention, _ := NewRetention(pool, objects, 1024)
	if count, err := retention.RecoverObjects(ctx, tenantA, 10); err != nil || count != 1 {
		t.Fatalf("recover count=%d err=%v", count, err)
	}
	newUpload := beginTestUpload(t, ctx, pool, objects, tenantA, "object_race")
	stored, err := newUpload.put(ctx, bytes.NewReader([]byte("new")), 1024)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = newUpload.publishTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("new"))
	if _, err = tx.Exec(ctx, `INSERT INTO managed_objects
		(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash)
		VALUES($1,$2,'output',$3,'application/octet-stream',$4,$5)`,
		tenantA, newUpload.id, stored.Key, stored.Size, digest[:]); err != nil {
		t.Fatal(err)
	}
	if err = newUpload.completeTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_ = newUpload.closeHeartbeat()
	staleTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = oldUpload.publishTx(ctx, staleTx); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale publish error=%v", err)
	}
	_ = staleTx.Rollback(ctx)
	if err = oldUpload.abort(); err != nil {
		t.Fatal(err)
	}
	reader, err := objects.Open(ctx, tenantA, newUpload.key)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(contents) != "new" {
		t.Fatalf("final=%q err=%v", contents, err)
	}
}

// 验证数据库 Commit 返回未知结果时，已存在 metadata 会阻止补偿逻辑删除 final 字节。
func TestObjectUploadAbortResolvesCommittedDatabaseState(t *testing.T) {
	ctx, pool, objects, tenantA, _ := objectRecoveryFixture(t)
	upload := beginTestUpload(t, ctx, pool, objects, tenantA, "object_committed")
	stored, err := upload.put(ctx, bytes.NewReader([]byte("committed")), 1024)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = upload.publishTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("committed"))
	if _, err = tx.Exec(ctx, `INSERT INTO managed_objects
		(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash)
		VALUES($1,$2,'output',$3,'application/octet-stream',$4,$5)`,
		tenantA, upload.id, stored.Key, stored.Size, digest[:]); err != nil {
		t.Fatal(err)
	}
	if err = upload.completeTx(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// This is the caller behavior after an ambiguous Commit error.
	if err = upload.abort(); err != nil {
		t.Fatal(err)
	}
	reader, err := objects.Open(ctx, tenantA, upload.key)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
}

// 验证删除在 durable deleting 状态后崩溃时可恢复，且恢复查询严格按租户隔离。
func TestManagedObjectDeletionRecoveryIsTenantScoped(t *testing.T) {
	ctx, pool, objects, tenantA, tenantB := objectRecoveryFixture(t)
	digest := sha256.Sum256([]byte("delete-me"))
	for index, tenantID := range []string{tenantA, tenantB} {
		kind := "output"
		if index == 0 {
			kind = "archive"
		}
		if _, err := objects.Put(ctx, tenantID, "deleting", bytes.NewReader([]byte("delete-me")), 1024); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO managed_objects
			(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,status)
			VALUES($1,'deleting',$2,'deleting','application/octet-stream',9,$3,'deleting')`,
			tenantID, kind, digest[:]); err != nil {
			t.Fatal(err)
		}
	}
	retention, _ := NewRetention(pool, objects, 1024)
	if count, err := retention.RecoverObjects(ctx, tenantA, 10); err != nil || count != 1 {
		t.Fatalf("recover count=%d err=%v", count, err)
	}
	var statusA, statusB string
	if err := pool.QueryRow(ctx, `SELECT status FROM managed_objects WHERE tenant_id=$1 AND id='deleting'`, tenantA).Scan(&statusA); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM managed_objects WHERE tenant_id=$1 AND id='deleting'`, tenantB).Scan(&statusB); err != nil {
		t.Fatal(err)
	}
	if statusA != "deleted" || statusB != "deleting" {
		t.Fatalf("statuses tenantA=%s tenantB=%s", statusA, statusB)
	}
	if _, err := objects.Open(ctx, tenantA, "deleting"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant A bytes remain: %v", err)
	}
	if reader, err := objects.Open(ctx, tenantB, "deleting"); err != nil {
		t.Fatalf("tenant B bytes removed: %v", err)
	} else {
		_ = reader.Close()
	}
	if _, err := objects.Put(ctx, tenantA, "expired_archive", bytes.NewReader([]byte("delete-me")), 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO managed_objects
		(tenant_id,id,kind,object_key,content_type,size_bytes,content_hash,status,archived_at)
		VALUES($1,'expired_archive','archive','expired_archive','application/octet-stream',9,$2,'archived',now()-interval '2 hours')`,
		tenantA, digest[:]); err != nil {
		t.Fatal(err)
	}
	if count, err := retention.DeleteArchived(ctx, tenantA, 0, 10); err != nil || count != 1 {
		t.Fatalf("delete archived kind count=%d err=%v", count, err)
	}
	if _, err := objects.Open(ctx, tenantA, "expired_archive"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("archive bytes remain: %v", err)
	}
}

// 验证 live lease 的判断完全由数据库时钟完成，控制面没有可注入的时间参数。
func TestObjectUploadRecoveryUsesDatabaseClock(t *testing.T) {
	ctx, pool, objects, tenantA, _ := objectRecoveryFixture(t)
	upload := beginTestUpload(t, ctx, pool, objects, tenantA, "object_live")
	if _, err := upload.put(ctx, bytes.NewReader([]byte("live")), 1024); err != nil {
		t.Fatal(err)
	}
	_ = upload.closeHeartbeat()
	retention, _ := NewRetention(pool, objects, 1024)
	if count, err := retention.RecoverObjects(ctx, tenantA, 10); err != nil || count != 0 {
		t.Fatalf("future database lease recovered count=%d err=%v", count, err)
	}
	path, _ := objects.path(tenantA, upload.key)
	if _, err := os.Stat(path + ".upload." + upload.ownerToken); err != nil {
		t.Fatalf("live stage removed: %v", err)
	}
	if err := upload.abort(); err != nil {
		t.Fatal(err)
	}
}

// 验证恢复器在物理删除后、事务提交前崩溃时会保留原 owner token，下一轮仍可幂等收敛 intent。
func TestObjectUploadRecoveryCrashPreservesImmutableOwner(t *testing.T) {
	ctx, pool, objects, tenantA, _ := objectRecoveryFixture(t)
	upload := beginTestUpload(t, ctx, pool, objects, tenantA, "object_recovery_crash")
	if _, err := upload.put(ctx, bytes.NewReader([]byte("recover twice")), 1024); err != nil {
		t.Fatal(err)
	}
	if err := upload.closeHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE object_upload_intents
		SET lease_expires_at=now()-interval '1 second'
		WHERE tenant_id=$1 AND id=$2`, tenantA, upload.id); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ownerToken, key string
	if err = tx.QueryRow(ctx, `SELECT owner_token,object_key FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantA, upload.id).Scan(&ownerToken, &key); err != nil {
		t.Fatal(err)
	}
	if err = objects.AbortUpload(ctx, tenantA, key, ownerToken, true); err != nil {
		t.Fatal(err)
	}
	// Rollback is the database-visible equivalent of a recovery-process crash.
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var preservedOwner string
	if err = pool.QueryRow(ctx, `SELECT owner_token FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2`, tenantA, upload.id).Scan(&preservedOwner); err != nil {
		t.Fatal(err)
	}
	if preservedOwner != upload.ownerToken {
		t.Fatalf("owner token changed across recovery rollback: got=%s want=%s", preservedOwner, upload.ownerToken)
	}
	retention, _ := NewRetention(pool, objects, 1024)
	if count, recoverErr := retention.RecoverObjects(ctx, tenantA, 10); recoverErr != nil || count != 1 {
		t.Fatalf("second recovery count=%d err=%v", count, recoverErr)
	}
	var remaining int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM object_upload_intents
		WHERE tenant_id=$1 AND id=$2`, tenantA, upload.id).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("remaining intents=%d err=%v", remaining, err)
	}
}
