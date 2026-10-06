package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/schema"
)

// histV1..histV3 build a history where the latest version can accept a
// candidate that an older version must reject (under BACKWARD):
//
//	v1: {a string, b int}      b is required int
//	v2: {a string}             b removed (allowed under BACKWARD)
//
// A candidate re-adding b as a different, non-widening type passes against
// v2 (pure optional addition) but fails against v1 (int->string unresolved).
var (
	histV1 = schema.Schema{Fields: []schema.Field{
		field("a", schema.KindString, false),
		field("b", schema.KindInt, false),
	}}
	histV2 = schema.Schema{Fields: []schema.Field{
		field("a", schema.KindString, false),
	}}
	trickyCandidate = schema.Schema{Fields: []schema.Field{
		field("a", schema.KindString, false),
		field("b", schema.KindString, true),
	}}
)

func TestScopedAuditRegistration(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	// Default scope of a fresh topic is LATEST.
	if _, err := r.Register(ctx, "def", histV1); err != nil {
		t.Fatalf("register def v1: %v", err)
	}
	if _, err := r.Register(ctx, "def", histV2); err != nil {
		t.Fatalf("register def v2: %v", err)
	}
	cfg, err := r.TopicConfig(ctx, "def")
	if err != nil {
		t.Fatalf("topic config: %v", err)
	}
	t.Logf("default scope of new topic: %s", cfg.Scope)
	if cfg.Scope.Kind != schema.ScopeLatest {
		t.Fatalf("default scope = %s, want LATEST", cfg.Scope)
	}
	// Under LATEST the tricky candidate only faces v2 and is accepted.
	res, err := r.Register(ctx, "def", trickyCandidate)
	if err != nil {
		t.Fatalf("under LATEST scope the candidate must pass, got %v", err)
	}
	t.Logf("LATEST scope accepted candidate as v%d", res.Version)

	// Same history on a second topic, but audited against ALL versions.
	if _, err := r.Register(ctx, "scoped", histV1); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if _, err := r.Register(ctx, "scoped", histV2); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	if err := r.SetAuditScope(ctx, "scoped", schema.AuditScope{Kind: schema.ScopeAll}); err != nil {
		t.Fatalf("set scope ALL: %v", err)
	}
	_, err = r.Register(ctx, "scoped", trickyCandidate)
	logErr(t, "register under scope ALL (b re-added as string)", err)
	if codeOf(err) != ErrIncompatibleSchema {
		t.Fatalf("expected INCOMPATIBLE_SCHEMA, got %v", err)
	}
	var re *Error
	if !errors.As(err, &re) {
		t.Fatalf("expected classified error, got %v", err)
	}
	joined := strings.Join(re.Reasons, "\n")
	t.Logf("rejection reasons:\n%s", joined)
	if !strings.Contains(joined, "baseline v1") {
		t.Fatalf("rejection must name blocking baseline v1, reasons: %v", re.Reasons)
	}
	if !strings.Contains(joined, `"b"`) || !strings.Contains(joined, "int->string") {
		t.Fatalf("rejection must name the triggering change on field b, reasons: %v", re.Reasons)
	}
	if !strings.Contains(re.Message, "[1]") {
		t.Fatalf("error message must list blocking versions, got %q", re.Message)
	}

	// A fully compatible candidate still registers under ALL.
	ok := schema.Schema{Fields: []schema.Field{
		field("a", schema.KindString, false),
		field("c", schema.KindBool, true),
	}}
	res, err = r.Register(ctx, "scoped", ok)
	if err != nil {
		t.Fatalf("compatible candidate under ALL must pass: %v", err)
	}
	t.Logf("ALL scope accepted compatible candidate as v%d", res.Version)

	// RECENT(1) behaves like LATEST again for this history.
	if err := r.SetAuditScope(ctx, "scoped", schema.AuditScope{Kind: schema.ScopeRecent, N: 1}); err != nil {
		t.Fatalf("set scope RECENT(1): %v", err)
	}
	res, err = r.Register(ctx, "scoped", trickyCandidate)
	if err != nil {
		t.Fatalf("under RECENT(1) the candidate only faces the latest version: %v", err)
	}
	t.Logf("RECENT(1) scope accepted candidate as v%d", res.Version)

	// Invalid scopes are rejected explicitly.
	err = r.SetAuditScope(ctx, "scoped", schema.AuditScope{Kind: schema.ScopeRecent, N: 0})
	logErr(t, "set RECENT(0)", err)
	if codeOf(err) != ErrInvalidScope {
		t.Fatalf("expected INVALID_SCOPE, got %v", err)
	}
	err = r.SetAuditScope(ctx, "scoped", schema.AuditScope{Kind: "SOMETIMES"})
	logErr(t, "set unknown scope kind", err)
	if codeOf(err) != ErrInvalidScope {
		t.Fatalf("expected INVALID_SCOPE, got %v", err)
	}
}

func TestPrecheckReportsPerVersion(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	for i, s := range []schema.Schema{histV1, histV2} {
		if _, err := r.Register(ctx, "pre", s); err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
	}
	if err := r.SetAuditScope(ctx, "pre", schema.AuditScope{Kind: schema.ScopeAll}); err != nil {
		t.Fatalf("set scope: %v", err)
	}

	report, err := r.Precheck(ctx, "pre", trickyCandidate, PrecheckOptions{})
	if err != nil {
		t.Fatalf("precheck: %v", err)
	}
	t.Logf("precheck report: strategy=%s scope=%s compatible=%v verdicts=%d",
		report.Strategy, report.Scope, report.Compatible, len(report.Verdicts))
	for _, v := range report.Verdicts {
		t.Logf("  baseline v%d: compatible=%v changes=%v reasons=%v", v.Version, v.Compatible, v.Changes, v.Reasons)
	}
	if report.Compatible {
		t.Fatalf("report must be incompatible overall")
	}
	if len(report.Verdicts) != 2 {
		t.Fatalf("scope ALL must yield 2 verdicts, got %d", len(report.Verdicts))
	}
	if report.Verdicts[0].Version != 1 || report.Verdicts[1].Version != 2 {
		t.Fatalf("verdicts must be ordered by version ascending, got %+v", report.Verdicts)
	}
	if report.Verdicts[0].Compatible {
		t.Fatalf("v1 verdict must be incompatible")
	}
	if len(report.Verdicts[0].Reasons) == 0 || len(report.Verdicts[0].Changes) == 0 {
		t.Fatalf("v1 verdict must carry reasons and changes: %+v", report.Verdicts[0])
	}
	if !report.Verdicts[1].Compatible {
		t.Fatalf("v2 verdict must be compatible, reasons: %v", report.Verdicts[1].Reasons)
	}

	// Strategy/scope overrides: LATEST-only precheck of the same candidate
	// passes, proving overrides do not touch the stored config.
	latestOnly := schema.AuditScope{Kind: schema.ScopeLatest}
	report2, err := r.Precheck(ctx, "pre", trickyCandidate, PrecheckOptions{Scope: &latestOnly})
	if err != nil {
		t.Fatalf("precheck with override: %v", err)
	}
	if !report2.Compatible || len(report2.Verdicts) != 1 || report2.Verdicts[0].Version != 2 {
		t.Fatalf("LATEST override must pass against v2 only, got %+v", report2)
	}
	t.Logf("override scope LATEST: compatible=%v verdicts=%d", report2.Compatible, len(report2.Verdicts))
	cfg, _ := r.TopicConfig(ctx, "pre")
	if cfg.Scope.Kind != schema.ScopeAll {
		t.Fatalf("precheck override must not change stored scope, got %s", cfg.Scope)
	}

	// Malformed candidate: explicit error, never a verdict, never empty.
	_, err = r.Precheck(ctx, "pre", schema.Schema{Fields: []schema.Field{{Name: "x", Type: "decimal"}}}, PrecheckOptions{})
	logErr(t, "precheck malformed candidate", err)
	if codeOf(err) != ErrMalformedSchema {
		t.Fatalf("expected MALFORMED_SCHEMA, got %v", err)
	}
	_, err = r.Precheck(ctx, "pre", schema.Schema{}, PrecheckOptions{})
	logErr(t, "precheck empty candidate", err)
	if codeOf(err) != ErrMalformedSchema {
		t.Fatalf("expected MALFORMED_SCHEMA for empty schema, got %v", err)
	}

	// Missing topic / invalid overrides are classified errors.
	if _, err = r.Precheck(ctx, "nope", trickyCandidate, PrecheckOptions{}); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("expected TOPIC_NOT_FOUND, got %v", err)
	}
	bad := schema.AuditScope{Kind: schema.ScopeRecent, N: -2}
	if _, err = r.Precheck(ctx, "pre", trickyCandidate, PrecheckOptions{Scope: &bad}); codeOf(err) != ErrInvalidScope {
		t.Fatalf("expected INVALID_SCOPE, got %v", err)
	}
	if _, err = r.Precheck(ctx, "pre", trickyCandidate, PrecheckOptions{Strategy: "SIDEWAYS"}); codeOf(err) != ErrInvalidStrategy {
		t.Fatalf("expected INVALID_STRATEGY, got %v", err)
	}
}

func TestPrecheckIsReadOnly(t *testing.T) {
	r, path := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	for i, s := range []schema.Schema{histV1, histV2} {
		if _, err := r.Register(ctx, "ro", s); err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
	}
	if err := r.SetAuditScope(ctx, "ro", schema.AuditScope{Kind: schema.ScopeAll}); err != nil {
		t.Fatalf("set scope: %v", err)
	}
	if err := r.Confirm(ctx, "ro", "consumer-x", 1); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	snapBefore, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	// Both a failing and a passing precheck must leave every trace untouched.
	if _, err := r.Precheck(ctx, "ro", trickyCandidate, PrecheckOptions{}); err != nil {
		t.Fatalf("failing precheck: %v", err)
	}
	passing := schema.Schema{Fields: []schema.Field{
		field("a", schema.KindString, false),
		field("c", schema.KindBool, true),
	}}
	if _, err := r.Precheck(ctx, "ro", passing, PrecheckOptions{}); err != nil {
		t.Fatalf("passing precheck: %v", err)
	}

	latest, err := r.GetLatest(ctx, "ro")
	if err != nil || latest.Version != 2 {
		t.Fatalf("latest after prechecks = %+v err=%v, want v2", latest, err)
	}
	pos, err := r.GetConfirmation(ctx, "ro", "consumer-x")
	if err != nil || pos != 1 {
		t.Fatalf("confirmation after prechecks = %d err=%v, want 1", pos, err)
	}
	page, err := r.ListVersions(ctx, "ro", 0, 0)
	if err != nil || page.Total != 2 {
		t.Fatalf("history after prechecks = %+v err=%v, want total 2", page, err)
	}
	snapAfter, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot after: %v", err)
	}
	if string(snapBefore) != string(snapAfter) {
		t.Fatalf("snapshot file changed after prechecks\nbefore: %s\nafter: %s", snapBefore, snapAfter)
	}
	t.Logf("prechecks left state untouched: latest=v%d consumer-x@v%d total=%d snapshot=%d bytes unchanged",
		latest.Version, pos, page.Total, len(snapAfter))
}

func TestListVersionsStableAndPaged(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	fields := []schema.Field{field("id", schema.KindLong, false)}
	for i := 0; i < 5; i++ {
		if i > 0 {
			fields = append(fields, field(fmt.Sprintf("f%d", i), schema.KindString, true))
		}
		s := schema.Schema{Fields: append([]schema.Field{}, fields...)}
		if _, err := r.Register(ctx, "paged", s); err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
	}

	full, err := r.ListVersions(ctx, "paged", 0, 0)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if full.Total != 5 || len(full.Versions) != 5 {
		t.Fatalf("full page = %+v, want total 5 len 5", full)
	}
	for i, v := range full.Versions {
		if v.Version != i+1 {
			t.Fatalf("versions not ascending at %d: %+v", i, full.Versions)
		}
	}
	// Repeated queries over the same data are byte-identical in order.
	again, _ := r.ListVersions(ctx, "paged", 0, 0)
	for i := range full.Versions {
		if full.Versions[i].Fingerprint != again.Versions[i].Fingerprint {
			t.Fatalf("unstable order between repeated queries")
		}
	}
	t.Logf("full history ascending and stable: total=%d", full.Total)

	cases := []struct {
		offset, limit int
		wantVersions  []int
		wantOffset    int
	}{
		{0, 2, []int{1, 2}, 0},
		{2, 2, []int{3, 4}, 2},
		{4, 2, []int{5}, 4},          // partial last page
		{5, 2, nil, 5},               // offset exactly at the end
		{99, 2, nil, 5},              // offset beyond the end is clamped
		{-3, 2, []int{1, 2}, 0},      // negative offset clamps to 0
		{1, 0, []int{2, 3, 4, 5}, 1}, // limit 0 = everything from offset
	}
	for _, tc := range cases {
		page, err := r.ListVersions(ctx, "paged", tc.offset, tc.limit)
		if err != nil {
			t.Fatalf("page offset=%d limit=%d: %v", tc.offset, tc.limit, err)
		}
		var got []int
		for _, v := range page.Versions {
			got = append(got, v.Version)
		}
		t.Logf("offset=%d limit=%d -> versions=%v total=%d", tc.offset, tc.limit, got, page.Total)
		if fmt.Sprint(got) != fmt.Sprint(tc.wantVersions) || page.Total != 5 || page.Offset != tc.wantOffset {
			t.Fatalf("offset=%d limit=%d: got versions=%v total=%d offset=%d, want versions=%v total=5 offset=%d",
				tc.offset, tc.limit, got, page.Total, page.Offset, tc.wantVersions, tc.wantOffset)
		}
	}

	if _, err := r.ListVersions(ctx, "missing", 0, 0); codeOf(err) != ErrTopicNotFound {
		t.Fatalf("expected TOPIC_NOT_FOUND, got %v", err)
	}
}

func TestConsumersBehindStableOrder(t *testing.T) {
	r, _ := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	fields := []schema.Field{field("id", schema.KindLong, false)}
	for i := 0; i < 5; i++ {
		if i > 0 {
			fields = append(fields, field(fmt.Sprintf("f%d", i), schema.KindString, true))
		}
		s := schema.Schema{Fields: append([]schema.Field{}, fields...)}
		if _, err := r.Register(ctx, "lag", s); err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
	}
	// lags: c-b=4, c-d=4 (tie, name decides), c-a=2, c-e=1, c-f up to date.
	positions := map[string]int{"c-b": 1, "c-d": 1, "c-a": 3, "c-e": 4, "c-f": 5}
	for consumer, v := range positions {
		if err := r.Confirm(ctx, "lag", consumer, v); err != nil {
			t.Fatalf("confirm %s@%d: %v", consumer, v, err)
		}
	}

	want := []string{"c-b", "c-d", "c-a", "c-e"}
	for round := 0; round < 5; round++ {
		behind, err := r.ConsumersBehind(ctx, "lag")
		if err != nil {
			t.Fatalf("consumers behind: %v", err)
		}
		var got []string
		for _, b := range behind {
			got = append(got, fmt.Sprintf("%s(lag=%d)", b.Consumer, b.Latest-b.Confirmed))
		}
		t.Logf("round %d behind order: %v", round, got)
		var names []string
		for _, b := range behind {
			names = append(names, b.Consumer)
		}
		if fmt.Sprint(names) != fmt.Sprint(want) {
			t.Fatalf("round %d: order = %v, want %v (lag desc, name asc on ties)", round, names, want)
		}
	}
}

func TestScopeSurvivesRestart(t *testing.T) {
	r, path := newRegistry(t, schema.CompatBackward)
	ctx := context.Background()

	for i, s := range []schema.Schema{histV1, histV2} {
		if _, err := r.Register(ctx, "durable", s); err != nil {
			t.Fatalf("register v%d: %v", i+1, err)
		}
	}
	if err := r.SetAuditScope(ctx, "durable", schema.AuditScope{Kind: schema.ScopeAll}); err != nil {
		t.Fatalf("set scope: %v", err)
	}
	before, err := r.Precheck(ctx, "durable", trickyCandidate, PrecheckOptions{})
	if err != nil {
		t.Fatalf("precheck before restart: %v", err)
	}

	// Restart and confirm the scope did not silently fall back to default.
	r2, err := New(context.Background(), Options{Path: path, DefaultStrategy: schema.CompatBackward})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	cfg, err := r2.TopicConfig(ctx, "durable")
	if err != nil {
		t.Fatalf("config after restart: %v", err)
	}
	t.Logf("scope after restart: %s", cfg.Scope)
	if cfg.Scope.Kind != schema.ScopeAll {
		t.Fatalf("scope after restart = %s, want ALL (must not fall back to default)", cfg.Scope)
	}

	after, err := r2.Precheck(ctx, "durable", trickyCandidate, PrecheckOptions{})
	if err != nil {
		t.Fatalf("precheck after restart: %v", err)
	}
	if before.Compatible != after.Compatible || len(before.Verdicts) != len(after.Verdicts) {
		t.Fatalf("verdict changed across restart: before=%+v after=%+v", before, after)
	}
	for i := range before.Verdicts {
		if before.Verdicts[i].Compatible != after.Verdicts[i].Compatible ||
			fmt.Sprint(before.Verdicts[i].Reasons) != fmt.Sprint(after.Verdicts[i].Reasons) {
			t.Fatalf("verdict %d changed across restart: before=%+v after=%+v", i, before.Verdicts[i], after.Verdicts[i])
		}
	}
	t.Logf("verdicts identical across restart: compatible=%v blocking=v1", after.Compatible)

	// Registration is rejected by the same baseline after the restart.
	_, err = r2.Register(ctx, "durable", trickyCandidate)
	logErr(t, "register tricky candidate after restart", err)
	if codeOf(err) != ErrIncompatibleSchema {
		t.Fatalf("expected INCOMPATIBLE_SCHEMA after restart, got %v", err)
	}
	var re *Error
	if errors.As(err, &re) && !strings.Contains(strings.Join(re.Reasons, "\n"), "baseline v1") {
		t.Fatalf("post-restart rejection must still name baseline v1: %v", re.Reasons)
	}
}
