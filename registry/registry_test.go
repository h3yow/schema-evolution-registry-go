package registry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

func field(name string, k schema.Kind, optional bool) schema.Field {
	return schema.Field{Name: name, Type: k, Optional: optional}
}

func codeOf(err error) ErrorCode {
	var re *Error
	if errors.As(err, &re) {
		return re.Code
	}
	return ""
}

func logErr(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Logf("%s: no error", label)
		return
	}
	var re *Error
	if errors.As(err, &re) {
		t.Logf("%s: code=%s message=%s reasons=%v", label, re.Code, re.Message, re.Reasons)
	} else {
		t.Logf("%s: unclassified error: %v", label, err)
	}
}

func newRegistry(t *testing.T, strategy schema.Compatibility) (Registry, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.json")
	r, err := New(context.Background(), Options{Path: path, DefaultStrategy: strategy})
	if err != nil {
		t.Fatalf("open registry: %v", err)
	}
	return r, path
}

func TestRegisterMonotonicAndIdempotent(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	v1 := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false)}}
	res1, err := r.Register(ctx, "orders", v1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	t.Logf("registered topic=%s version=%d reused=%v fp=%s", res1.Topic, res1.Version, res1.Reused, res1.Fingerprint)
	if res1.Version != 1 || res1.Reused {
		t.Fatalf("expected version 1 fresh, got %+v", res1)
	}

	// Same content again: deterministic reuse, no new version.
	res2, err := r.Register(ctx, "orders", v1)
	if err != nil {
		t.Fatalf("re-register same content: %v", err)
	}
	t.Logf("duplicate submit -> version=%d reused=%v", res2.Version, res2.Reused)
	if res2.Version != 1 || !res2.Reused {
		t.Fatalf("expected reuse of version 1, got %+v", res2)
	}

	// Explicit version already occupied by different content: conflict.
	other := schema.Schema{Fields: []schema.Field{field("id", schema.KindString, false)}}
	_, err = r.RegisterVersion(ctx, "orders", 1, other)
	logErr(t, "re-register v1 with different content", err)
	if codeOf(err) != ErrVersionConflict {
		t.Fatalf("expected VERSION_CONFLICT, got %v", err)
	}

	// Explicit version skipping ahead: out of order.
	_, err = r.RegisterVersion(ctx, "orders", 5, other)
	logErr(t, "register v5 while next is v2", err)
	if codeOf(err) != ErrVersionOutOfOrder {
		t.Fatalf("expected VERSION_OUT_OF_ORDER, got %v", err)
	}

	// Next version advances monotonically.
	v2 := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false), field("email", schema.KindString, true)}}
	res3, err := r.RegisterVersion(ctx, "orders", 2, v2)
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}
	t.Logf("registered version=%d", res3.Version)
	if res3.Version != 2 {
		t.Fatalf("expected version 2, got %d", res3.Version)
	}

	latest, err := r.GetLatest(ctx, "orders")
	if err != nil || latest.Version != 2 {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	if _, err := r.GetVersion(ctx, "orders", 3); codeOf(err) != ErrVersionNotFound {
		t.Fatalf("expected VERSION_NOT_FOUND, got %v", err)
	}
	if _, err := r.GetVersion(ctx, "missing", 1); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("expected TOPIC_NOT_FOUND, got %v", err)
	}
}

func TestConcurrentRegistrationDeterministic(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	// N goroutines race to publish DIFFERENT contents for the SAME explicit
	// version 1. Exactly one may win; everyone else must see VERSION_CONFLICT
	// (or reuse if they submitted the winner's content). No two contents may
	// share version 1 and the winner may never be overwritten.
	const n = 16
	schemas := make([]schema.Schema, n)
	for i := 0; i < n; i++ {
		schemas[i] = schema.Schema{Fields: []schema.Field{
			field("id", schema.KindLong, false),
			field(fmt.Sprintf("tag_%02d", i), schema.KindString, true),
		}}
	}

	var wg sync.WaitGroup
	results := make([]RegistrationResult, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := r.RegisterVersion(ctx, "race", 1, schemas[i])
			results[i], errs[i] = res, err
		}(i)
	}
	wg.Wait()

	wins := 0
	winnerIdx := -1
	for i := 0; i < n; i++ {
		if errs[i] == nil {
			wins++
			winnerIdx = i
			if results[i].Version != 1 || results[i].Reused {
				t.Fatalf("winner %d got unexpected result %+v", i, results[i])
			}
			t.Logf("goroutine %02d WON version=%d fp=%s", i, results[i].Version, results[i].Fingerprint)
		} else {
			logErr(t, fmt.Sprintf("goroutine %02d lost", i), errs[i])
			if codeOf(errs[i]) != ErrVersionConflict {
				t.Fatalf("loser %d expected VERSION_CONFLICT, got %v", i, errs[i])
			}
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", wins)
	}

	// Concurrent duplicate submissions of the winner's content must all agree.
	var wg2 sync.WaitGroup
	dups := make([]RegistrationResult, n)
	dupErrs := make([]error, n)
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			dups[i], dupErrs[i] = r.Register(ctx, "race", schemas[winnerIdx])
		}(i)
	}
	wg2.Wait()
	for i := 0; i < n; i++ {
		if dupErrs[i] != nil {
			t.Fatalf("duplicate %d errored: %v", i, dupErrs[i])
		}
		if dups[i].Version != 1 {
			t.Fatalf("duplicate %d got version %d, want 1", i, dups[i].Version)
		}
	}
	t.Logf("all %d duplicate submissions resolved to version 1", n)

	// The published version 1 must be exactly the winner's content.
	got, err := r.GetVersion(ctx, "race", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if got.Fingerprint != schemas[winnerIdx].Fingerprint() {
		t.Fatalf("published v1 fingerprint %s does not match winner goroutine %d (%s)",
			got.Fingerprint, winnerIdx, schemas[winnerIdx].Fingerprint())
	}
	t.Logf("published v1 fingerprint=%s matches winner goroutine %02d", got.Fingerprint, winnerIdx)
}

func TestCompatibilityRejection(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	v1 := schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("name", schema.KindString, false),
	}}
	if _, err := r.Register(ctx, "users", v1); err != nil {
		t.Fatalf("register v1: %v", err)
	}

	// Required field added: rejected under BACKWARD with explainable reasons.
	bad := schema.Schema{Fields: append(append([]schema.Field{}, v1.Fields...), field("email", schema.KindString, false))}
	_, err := r.Register(ctx, "users", bad)
	logErr(t, "add required field under BACKWARD", err)
	if codeOf(err) != ErrIncompatibleSchema {
		t.Fatalf("expected INCOMPATIBLE_SCHEMA, got %v", err)
	}
	var re *Error
	if !errors.As(err, &re) || len(re.Reasons) == 0 {
		t.Fatalf("rejection must carry reasons: %v", err)
	}

	// Malformed schema: explicit error, never silently accepted.
	malformed := schema.Schema{Fields: []schema.Field{{Name: "x", Type: "decimal"}}}
	_, err = r.Register(ctx, "users", malformed)
	logErr(t, "register malformed schema", err)
	if codeOf(err) != ErrMalformedSchema {
		t.Fatalf("expected MALFORMED_SCHEMA, got %v", err)
	}

	// Compatible evolution succeeds and bumps the version.
	ok := schema.Schema{Fields: append(append([]schema.Field{}, v1.Fields...), field("email", schema.KindString, true))}
	res, err := r.Register(ctx, "users", ok)
	if err != nil {
		t.Fatalf("register compatible v2: %v", err)
	}
	t.Logf("compatible evolution accepted as version=%d", res.Version)

	// QueryByStrategy: candidate compatible with v2 resolves to v2.
	found, err := r.QueryByStrategy(ctx, "users", schema.CompatBackward, ok)
	if err != nil || found.Version != 2 {
		t.Fatalf("query by strategy: %+v err=%v", found, err)
	}
	t.Logf("query BACKWARD resolved to version=%d", found.Version)
}

func TestRestartRecoveryAndConfirmations(t *testing.T) {
	r, path := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	v1 := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false)}}
	v2 := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false), field("email", schema.KindString, true)}}
	v3 := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false), field("email", schema.KindString, true), field("phone", schema.KindString, true)}}
	for i, s := range []schema.Schema{v1, v2, v3} {
		res, err := r.Register(ctx, "events", s)
		if err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
		t.Logf("registered version=%d fp=%s", res.Version, res.Fingerprint)
	}

	// consumer-a lags at v1, consumer-b catches up to v3.
	if err := r.Confirm(ctx, "events", "consumer-a", 1); err != nil {
		t.Fatalf("confirm a@1: %v", err)
	}
	if err := r.Confirm(ctx, "events", "consumer-b", 3); err != nil {
		t.Fatalf("confirm b@3: %v", err)
	}

	// Rollback must be rejected before restart.
	err := r.Confirm(ctx, "events", "consumer-b", 2)
	logErr(t, "rollback consumer-b 3->2", err)
	if codeOf(err) != ErrInvalidConfirmation {
		t.Fatalf("expected INVALID_CONFIRMATION, got %v", err)
	}

	// Simulate process restart: reopen from the same file.
	r2, err := New(context.Background(), Options{Path: path, DefaultStrategy: schema.CompatBackward})
	if err != nil {
		t.Fatalf("reopen registry: %v", err)
	}

	posA, err := r2.GetConfirmation(ctx, "events", "consumer-a")
	if err != nil || posA != 1 {
		t.Fatalf("consumer-a position after restart = %d, err=%v; want 1", posA, err)
	}
	posB, err := r2.GetConfirmation(ctx, "events", "consumer-b")
	if err != nil || posB != 3 {
		t.Fatalf("consumer-b position after restart = %d, err=%v; want 3", posB, err)
	}
	t.Logf("after restart: consumer-a@v%d consumer-b@v%d (no loss, no rollback)", posA, posB)

	latest, err := r2.GetLatest(ctx, "events")
	if err != nil || latest.Version != 3 {
		t.Fatalf("latest after restart = %+v err=%v", latest, err)
	}

	// Lagging consumer is reported with its pending versions.
	behind, err := r2.ConsumersBehind(ctx, "events")
	if err != nil {
		t.Fatalf("consumers behind: %v", err)
	}
	if len(behind) != 1 || behind[0].Consumer != "consumer-a" {
		t.Fatalf("expected only consumer-a behind, got %+v", behind)
	}
	t.Logf("behind: consumer=%s confirmed=v%d latest=v%d pending=%v",
		behind[0].Consumer, behind[0].Confirmed, behind[0].Latest, behind[0].PendingVersions)
	if fmt.Sprint(behind[0].PendingVersions) != "[2 3]" {
		t.Fatalf("pending versions = %v, want [2 3]", behind[0].PendingVersions)
	}

	// Rollback is still rejected after restart (position did not reset).
	err = r2.Confirm(ctx, "events", "consumer-a", 0)
	logErr(t, "confirm version 0 after restart", err)
	if codeOf(err) != ErrVersionNotFound {
		t.Fatalf("expected VERSION_NOT_FOUND, got %v", err)
	}

	// Registration after restart continues the version sequence.
	v4 := schema.Schema{Fields: append(append([]schema.Field{}, v3.Fields...), field("note", schema.KindString, true))}
	res, err := r2.Register(ctx, "events", v4)
	if err != nil || res.Version != 4 {
		t.Fatalf("register after restart = %+v err=%v; want version 4", res, err)
	}
	t.Logf("post-restart registration assigned version=%d", res.Version)

	// Idempotent confirm of the same version is fine.
	if err := r2.Confirm(ctx, "events", "consumer-a", 1); err != nil {
		t.Fatalf("idempotent confirm: %v", err)
	}
}

func TestCorruptSnapshotFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	if err := writeFile(path, []byte("{not json")); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}
	_, err := New(context.Background(), Options{Path: path})
	logErr(t, "open registry over corrupt snapshot", err)
	if codeOf(err) != ErrPersistence {
		t.Fatalf("expected PERSISTENCE_FAILED, got %v", err)
	}
}
