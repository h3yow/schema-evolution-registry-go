package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/schema"
	"github.com/h3yow/schema-evolution-registry-go/storage"
)

// Scenarios covered by this file (the banner is printed in verbose logs so the
// new-capability coverage is visible at a glance):
//
//  1. cross-version range review: a candidate compatible with the latest
//     version but incompatible with an old one passes under LATEST and is
//     rejected under ALL/RECENT with the exact blocking version named;
//  2. preflight is strictly read-only and lists one verdict per reviewed
//     version, including the reasons for each rejection;
//  3. malformed/empty preflight candidates are classified errors, never empty
//     or compatible reports;
//  4. version history ordering and pagination boundaries are stable;
//  5. lagging-consumer ordering is stable (lag desc, consumer name asc);
//  6. the configured scope and the verdict conclusions survive a restart.
func init() {
	fmt.Println("[scope_test] scenarios: range-review-pass/reject-with-blocking-version | read-only-preflight | malformed-candidate | stable-pagination | stable-consumer-order | restart-scope-persistence")
}

// seedHistory registers v1..vn on topic; each version adds one OPTIONAL field,
// which stays legal under BACKWARD against every earlier version.
func seedHistory(t *testing.T, r Registry, topic string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		fields := []schema.Field{field("id", schema.KindLong, false)}
		for j := 1; j < i; j++ {
			fields = append(fields, field(fmt.Sprintf("opt_%d", j), schema.KindString, true))
		}
		if _, err := r.Register(ctx, topic, schema.Schema{Fields: fields}); err != nil {
			t.Fatalf("seed v%d: %v", i, err)
		}
	}
}

// lagBreakerHistory is the user's scenario in miniature:
//
//	v1 = {id:long, code:int required}
//	v2 = {id:long}                       -- dropping required code is legal BACKWARD vs v1
//	candidate = {id:long, code:string optional}
//	  vs v2: an optional field added -> COMPATIBLE (passes the latest)
//	  vs v1: field "code" int->string is unresolved -> REJECTED (kills v1 consumers)
func lagBreakerSchemas() (v1, v2, candidate schema.Schema) {
	v1 = schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("code", schema.KindInt, false),
	}}
	v2 = schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false)}}
	candidate = schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("code", schema.KindString, true),
	}}
	return v1, v2, candidate
}

func TestReviewScopeRangeReview(t *testing.T) {
	ctx := context.Background()
	const topic = "events"

	// --- LATEST (default): only v2 is reviewed, the candidate sails through.
	r, _ := newRegistry(t, schema.CompatBackward)
	v1, v2, candidate := lagBreakerSchemas()
	if _, err := r.Register(ctx, topic, v1); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if _, err := r.Register(ctx, topic, v2); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	info, err := r.GetTopic(ctx, topic)
	if err != nil {
		t.Fatalf("get topic: %v", err)
	}
	if info.Scope.Mode != storage.ScopeLatest {
		t.Fatalf("default scope = %s, want LATEST", info.Scope.Mode)
	}
	res, err := r.Register(ctx, topic, candidate)
	if err != nil {
		t.Fatalf("LATEST review should pass vs v2, got %v", err)
	}
	t.Logf("LATEST scope: candidate accepted as v%d (a consumer still on v1 would break)", res.Version)

	// --- ALL: the same candidate is rejected and v1 is named as the blocker.
	r2, _ := newRegistry(t, schema.CompatBackward)
	if _, err := r2.Register(ctx, topic, v1); err != nil {
		t.Fatalf("seed v1: %v", err)
	}
	if _, err := r2.Register(ctx, topic, v2); err != nil {
		t.Fatalf("seed v2: %v", err)
	}
	if err := r2.ConfigureScope(ctx, topic, storage.AllScope()); err != nil {
		t.Fatalf("configure ALL: %v", err)
	}
	_, err = r2.Register(ctx, topic, candidate)
	logErr(t, "ALL scope rejects candidate that breaks v1", err)
	if codeOf(err) != ErrIncompatibleSchema {
		t.Fatalf("expected INCOMPATIBLE_SCHEMA under ALL, got %v", err)
	}
	var re *Error
	if !errors.As(err, &re) {
		t.Fatalf("rejection is not registry.Error")
	}
	if len(re.Blocking) != 1 || re.Blocking[0].Version != 1 {
		t.Fatalf("want exactly one blocking verdict at v1, got %+v", re.Blocking)
	}
	if len(re.Blocking[0].Reasons) == 0 {
		t.Fatalf("blocking verdict must explain the trigger: %+v", re.Blocking[0])
	}
	t.Logf("ALL scope: blocked at v%d (%d blocking verdict), reasons=%v",
		re.Blocking[0].Version, len(re.Blocking), re.Reasons)

	// --- RECENT(1): v1 leaves the window, behaviour matches LATEST.
	if err := r2.ConfigureScope(ctx, topic, storage.RecentScope(1)); err != nil {
		t.Fatalf("configure RECENT(1): %v", err)
	}
	if _, err := r2.Register(ctx, topic, candidate); err != nil {
		t.Fatalf("RECENT(1) should review only v2 and pass, got %v", err)
	}
	t.Logf("RECENT(1) scope: candidate accepted (v1 outside window)")

	// --- bad scope inputs are refused, never silently applied.
	if err := r2.ConfigureScope(ctx, topic, storage.RecentScope(0)); codeOf(err) != ErrInvalidScope {
		t.Fatalf("RECENT(0): expected INVALID_SCOPE, got %v", err)
	}
	if err := r2.ConfigureScope(ctx, "missing-topic", storage.AllScope()); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("configure unknown topic: expected TOPIC_NOT_FOUND, got %v", err)
	}
	t.Logf("invalid scope (RECENT limit=0) and unknown topic rejected with classified errors")
}

func TestPreflightReadOnlyAndExplained(t *testing.T) {
	r, path := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()
	const topic = "events"
	seedHistory(t, r, topic, 3)
	if err := r.Confirm(ctx, topic, "consumer-a", 1); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if err := r.ConfigureScope(ctx, topic, storage.AllScope()); err != nil {
		t.Fatalf("configure ALL: %v", err)
	}
	// Baseline snapshot bytes are captured AFTER setup, immediately before the
	// read-only preflight sequence.
	snapshot, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	// Legal vs every version: a full green report, one verdict per version.
	good := schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("opt_1", schema.KindString, true),
		field("opt_2", schema.KindString, true),
		field("preview", schema.KindString, true),
	}}
	report, err := r.Preflight(ctx, topic, good, PreflightOptions{})
	if err != nil {
		t.Fatalf("preflight good candidate: %v", err)
	}
	if !report.Compatible || report.BlockingVersion != 0 || !report.WouldRegister {
		t.Fatalf("unexpected report: %+v", report)
	}
	if fmt.Sprint(report.CheckedVersions) != "[1 2 3]" || len(report.Verdicts) != 3 {
		t.Fatalf("expected verdicts for v1..v3 ascending, got %+v", report.CheckedVersions)
	}
	t.Logf("preflight compatible: checked=%v verdicts=%d", report.CheckedVersions, len(report.Verdicts))

	// Required add under BACKWARD: blocked already at v1, yet every in-scope
	// verdict (with reasons) is still returned.
	bad := schema.Schema{Fields: []schema.Field{
		field("id", schema.KindLong, false),
		field("opt_1", schema.KindString, true),
		field("opt_2", schema.KindString, true),
		field("preview", schema.KindString, false),
	}}
	report, err = r.Preflight(ctx, topic, bad, PreflightOptions{})
	if err != nil {
		t.Fatalf("an incompatible verdict belongs in the report, got error: %v", err)
	}
	if report.Compatible || report.BlockingVersion != 1 {
		t.Fatalf("expected blocked at v1, got %+v", report)
	}
	if len(report.Verdicts) != 3 {
		t.Fatalf("all in-scope verdicts must be listed even after a block, got %d", len(report.Verdicts))
	}
	for _, vd := range report.Verdicts {
		if vd.Compatible {
			t.Logf("preflight v%d: compatible", vd.Version)
		} else {
			t.Logf("preflight v%d: REJECTED reasons=%v changes=%v", vd.Version, vd.Reasons, vd.Changes)
		}
	}

	// Scope override narrows the window for this one call and does not persist.
	report, err = r.Preflight(ctx, topic, bad, PreflightOptions{Scope: storage.RecentScope(1)})
	if err != nil {
		t.Fatalf("preflight override: %v", err)
	}
	if fmt.Sprint(report.CheckedVersions) != "[3]" {
		t.Fatalf("RECENT(1) should only review v3, got %v", report.CheckedVersions)
	}
	info, _ := r.GetTopic(ctx, topic)
	if info.Scope.Mode != storage.ScopeAll {
		t.Fatalf("preflight override must not persist scope, got %s", info.Scope.Mode)
	}
	t.Logf("preflight scope override RECENT(1) checked=%v; persisted scope stays %s",
		report.CheckedVersions, info.Scope)

	// Strictly read-only: snapshot bytes, version frontier and confirmation all
	// unchanged after several preflight calls.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot after: %v", err)
	}
	if string(snapshot) != string(after) {
		t.Fatal("snapshot file changed during read-only preflight")
	}
	latest, _ := r.GetLatest(ctx, topic)
	if latest.Version != 3 {
		t.Fatalf("preflight published a version: latest=%d", latest.Version)
	}
	if pos, err := r.GetConfirmation(ctx, topic, "consumer-a"); err != nil || pos != 1 {
		t.Fatalf("confirmation changed during preflight: pos=%d err=%v", pos, err)
	}
	t.Logf("preflight changed no state: latest stays v3, consumer-a stays @v1, snapshot bytes identical")

	// Existing content is reported as reuse, not as a new registration.
	existing, err := r.GetVersion(ctx, topic, 2)
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	report, err = r.Preflight(ctx, topic, existing.Schema, PreflightOptions{})
	if err != nil || report.WouldRegister || report.ReusedVersion != 2 {
		t.Fatalf("expected reuse v2, got %+v err=%v", report, err)
	}
	t.Logf("preflight duplicate content -> ReusedVersion=%d WouldRegister=%v",
		report.ReusedVersion, report.WouldRegister)
}

func TestPreflightMalformedCandidate(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()
	const topic = "events"
	seedHistory(t, r, topic, 2)

	malformed := schema.Schema{Fields: []schema.Field{{Name: "id", Type: "decimal"}}}
	report, err := r.Preflight(ctx, topic, malformed, PreflightOptions{})
	logErr(t, "preflight malformed candidate", err)
	if codeOf(err) != ErrMalformedSchema {
		t.Fatalf("expected MALFORMED_SCHEMA, got report=%+v err=%v", report, err)
	}
	if len(report.Verdicts) != 0 || report.Compatible {
		t.Fatalf("malformed candidate must not yield a compatible/non-empty report: %+v", report)
	}

	if _, err := r.Preflight(ctx, topic, schema.Schema{}, PreflightOptions{}); codeOf(err) != ErrMalformedSchema {
		t.Fatalf("empty schema: expected MALFORMED_SCHEMA, got %v", err)
	}

	// Bad overrides and unknown topics are classified, never silently defaulted.
	good := schema.Schema{Fields: []schema.Field{field("id", schema.KindLong, false)}}
	if _, err := r.Preflight(ctx, topic, good,
		PreflightOptions{Strategy: "SIDEWAYS"}); codeOf(err) != ErrInvalidStrategy {
		t.Fatalf("bad strategy override: expected INVALID_STRATEGY, got %v", err)
	}
	if _, err := r.Preflight(ctx, topic, good,
		PreflightOptions{Scope: storage.RecentScope(-1)}); codeOf(err) != ErrInvalidScope {
		t.Fatalf("bad scope override: expected INVALID_SCOPE, got %v", err)
	}
	if _, err := r.Preflight(ctx, "ghost", good, PreflightOptions{}); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("unknown topic: expected TOPIC_NOT_FOUND, got %v", err)
	}
	t.Logf("malformed/empty candidates and bad overrides all return classified errors, never empty green reports")
}

func TestListVersionsPagination(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()
	const topic = "p"
	seedHistory(t, r, topic, 5)

	collect := func(limit int) []int {
		var out []int
		cursor := 0
		for {
			page, err := r.ListVersions(ctx, topic, cursor, limit)
			if err != nil {
				t.Fatalf("list cursor=%d limit=%d: %v", cursor, limit, err)
			}
			for _, v := range page.Versions {
				out = append(out, v.Version)
			}
			if page.NextCursor == 0 {
				break
			}
			if page.NextCursor <= cursor {
				t.Fatalf("cursor must advance: %d -> %d", cursor, page.NextCursor)
			}
			cursor = page.NextCursor
		}
		return out
	}

	for _, limit := range []int{2, 3, 5, 100} {
		got := collect(limit)
		if fmt.Sprint(got) != "[1 2 3 4 5]" {
			t.Fatalf("limit=%d pagination = %v, want [1 2 3 4 5]", limit, got)
		}
		t.Logf("limit=%d pages concatenate to %v", limit, got)
	}

	// Exact-fit page (limit == total) exhausts with no next cursor.
	page, err := r.ListVersions(ctx, topic, 0, 5)
	if err != nil || len(page.Versions) != 5 || page.NextCursor != 0 {
		t.Fatalf("exact fit page = %+v err=%v", page, err)
	}
	// Boundary: size-2 pages end at v2 and resume exactly at v3.
	page, err = r.ListVersions(ctx, topic, 0, 2)
	if err != nil || len(page.Versions) != 2 || page.Versions[0].Version != 1 ||
		page.Versions[1].Version != 2 || page.NextCursor != 2 {
		t.Fatalf("first size-2 page = %+v err=%v", page, err)
	}
	page, err = r.ListVersions(ctx, topic, page.NextCursor, 2)
	if err != nil || len(page.Versions) != 2 || page.Versions[0].Version != 3 ||
		page.Versions[1].Version != 4 || page.NextCursor != 4 {
		t.Fatalf("second size-2 page = %+v err=%v", page, err)
	}
	// limit<=0 falls back to the default; oversized limits are clamped.
	page, err = r.ListVersions(ctx, topic, 0, 0)
	if err != nil || page.Limit != defaultVersionPageLimit || len(page.Versions) != 5 {
		t.Fatalf("default-limit page = %+v err=%v", page, err)
	}
	page, err = r.ListVersions(ctx, topic, 0, maxVersionPageLimit+50)
	if err != nil || page.Limit != maxVersionPageLimit {
		t.Fatalf("limit should be clamped to %d, got %d", maxVersionPageLimit, page.Limit)
	}
	if _, err := r.ListVersions(ctx, topic, -1, 10); codeOf(err) != ErrInvalidPage {
		t.Fatalf("negative cursor: expected INVALID_PAGE, got %v", err)
	}
	if _, err := r.ListVersions(ctx, "ghost", 0, 10); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("unknown topic: expected TOPIC_NOT_FOUND, got %v", err)
	}
	t.Logf("pagination boundaries stable: exact-fit exhausts, cursor resumes precisely, defaults/clamps apply")
}

func TestConsumersBehindStableOrder(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()
	const topic = "q"
	seedHistory(t, r, topic, 5)

	confirms := map[string]int{
		"zeta":   1, // lag 4
		"alpha":  1, // lag 4 — tie broken by consumer name
		"mid":    3, // lag 2
		"caught": 5, // up to date, excluded
	}
	for c, v := range confirms {
		if err := r.Confirm(ctx, topic, c, v); err != nil {
			t.Fatalf("confirm %s@v%d: %v", c, v, err)
		}
	}

	var prev []string
	for iter := 0; iter < 3; iter++ {
		got, err := r.ConsumersBehind(ctx, topic)
		if err != nil {
			t.Fatalf("consumers behind iter %d: %v", iter, err)
		}
		labels := make([]string, len(got))
		for i, v := range got {
			labels[i] = fmt.Sprintf("%s@v%d", v.Consumer, v.Confirmed)
		}
		t.Logf("iteration %d order: %v", iter+1, labels)
		want := "[alpha@v1 zeta@v1 mid@v3]"
		if fmt.Sprint(labels) != want {
			t.Fatalf("order = %v, want %s (lag desc, name asc)", labels, want)
		}
		if fmt.Sprint(got[2].PendingVersions) != "[4 5]" {
			t.Fatalf("mid pending = %v, want [4 5]", got[2].PendingVersions)
		}
		if prev != nil && fmt.Sprint(prev) != fmt.Sprint(labels) {
			t.Fatalf("non-deterministic ordering: %v vs %v", prev, labels)
		}
		prev = labels
	}
	t.Logf("consumer lag ordering stable across repeated queries: lag desc, name asc on ties")
}

func TestScopePersistenceAcrossRestart(t *testing.T) {
	r, path := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()
	const topic = "events"
	v1, v2, candidate := lagBreakerSchemas()
	if _, err := r.Register(ctx, topic, v1); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if _, err := r.Register(ctx, topic, v2); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if err := r.ConfigureScope(ctx, topic, storage.RecentScope(2)); err != nil {
		t.Fatalf("configure RECENT(2): %v", err)
	}

	// Restart: scope must come back as RECENT(2), not the LATEST default.
	r2, err := New(context.Background(), Options{Path: path, DefaultStrategy: schema.CompatBackward})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	info, err := r2.GetTopic(ctx, topic)
	if err != nil {
		t.Fatalf("get topic after restart: %v", err)
	}
	if info.Scope.Mode != storage.ScopeRecent || info.Scope.Limit != 2 {
		t.Fatalf("scope after restart = %s, want RECENT(2)", info.Scope)
	}
	t.Logf("after restart scope recovered as %s (did not revert to default)", info.Scope)

	// Under the recovered window both v1 and v2 are reviewed: same rejection
	// the first process would have produced, named at v1.
	report, err := r2.Preflight(ctx, topic, candidate, PreflightOptions{})
	if err != nil {
		t.Fatalf("preflight after restart: %v", err)
	}
	if report.Compatible || report.BlockingVersion != 1 ||
		fmt.Sprint(report.CheckedVersions) != "[1 2]" {
		t.Fatalf("post-restart verdict mismatch: %+v", report)
	}
	t.Logf("after restart preflight under %s checked=%v blocked@v%d — identical conclusion",
		info.Scope, report.CheckedVersions, report.BlockingVersion)

	_, err = r2.Register(ctx, topic, candidate)
	logErr(t, "register after restart under recovered scope", err)
	if codeOf(err) != ErrIncompatibleSchema {
		t.Fatalf("expected INCOMPATIBLE_SCHEMA after restart, got %v", err)
	}

	// Narrowing the window post-restart persists and lets the candidate pass.
	if err := r2.ConfigureScope(ctx, topic, storage.RecentScope(1)); err != nil {
		t.Fatalf("configure RECENT(1): %v", err)
	}
	res, err := r2.Register(ctx, topic, candidate)
	if err != nil || res.Version != 3 {
		t.Fatalf("RECENT(1) after restart should pass vs v2 as v3, got %+v err=%v", res, err)
	}
	t.Logf("post-restart scope change to RECENT(1) persisted; candidate accepted as v3")
}
