package registry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/internal/compat"
)

func schema(fields ...string) []byte {
	body := ""
	for i, f := range fields {
		if i > 0 {
			body += ","
		}
		body += f
	}
	return []byte(fmt.Sprintf(`{"name":"E","fields":[%s]}`, body))
}

var (
	fID    = `{"name":"id","type":"long"}`
	fName  = `{"name":"name","type":"string"}`
	fNameN = `{"name":"nick","type":"string","default":""}`
	fAgeD  = `{"name":"age","type":"int","default":0}`
)

func openTestRegistry(t *testing.T, strategy compat.Strategy) (*Registry, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "registry.json")
	r, err := Open(context.Background(), path, WithStrategy(strategy))
	if err != nil {
		t.Fatal(err)
	}
	return r, path
}

func TestRegisterMonotonicAndIdempotent(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "users"

	v1raw := schema(fID, fName)
	v1, existed, err := r.Register(ctx, topic, v1raw)
	if err != nil || v1 != 1 || existed {
		t.Fatalf("first register: v=%d existed=%v err=%v", v1, existed, err)
	}

	// Add an optional field (with default): compatible backward.
	v2raw := schema(fID, fName, fNameN)
	v2, existed, err := r.Register(ctx, topic, v2raw)
	if err != nil || v2 != 2 || existed {
		t.Fatalf("second register: v=%d existed=%v err=%v", v2, existed, err)
	}

	// Re-submit identical content -> same version, idempotent, no new version.
	v2b, existed, err := r.Register(ctx, topic, v2raw)
	if err != nil || v2b != 2 || !existed {
		t.Fatalf("identical re-register must be idempotent: v=%d existed=%v err=%v", v2b, existed, err)
	}

	versions, err := r.Versions(ctx, topic)
	if err != nil || len(versions) != 2 {
		t.Fatalf("expected exactly 2 versions after duplicate, got %d err=%v", len(versions), err)
	}
	if versions[0].SchemaID == versions[1].SchemaID {
		t.Fatal("distinct content must not share a schema id")
	}

	// Queries by version and latest.
	if got, err := r.Latest(ctx, topic); err != nil || got.Version != 2 {
		t.Fatalf("latest wrong: %+v err=%v", got, err)
	}
	if got, err := r.GetVersion(ctx, topic, 1); err != nil || got.Schema.Name != "E" {
		t.Fatalf("get v1 wrong: %+v err=%v", got, err)
	}
}

func TestRegisterVersionConflict(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "t"

	v1 := schema(fID, fName)
	if err := r.RegisterVersion(ctx, topic, 1, v1); err != nil {
		t.Fatal(err)
	}
	// Same version, same content -> idempotent.
	if err := r.RegisterVersion(ctx, topic, 1, v1); err != nil {
		t.Fatalf("same content at same version must be idempotent: %v", err)
	}
	// Same version, different content -> VERSION_EXISTS, no silent overwrite.
	different := schema(fID, fNameN)
	err := r.RegisterVersion(ctx, topic, 1, different)
	if ClassOf(err) != ClassVersionExists {
		t.Fatalf("expected VERSION_EXISTS, got class=%s err=%v", ClassOf(err), err)
	}
	got, _ := r.GetVersion(ctx, topic, 1)
	if got.SchemaID == mustID(t, different) {
		t.Fatal("published version must not be overwritten by conflicting content")
	}
	// Gap is rejected.
	if err := r.RegisterVersion(ctx, topic, 3, schema(fID, fName, fNameN)); ClassOf(err) != ClassVersionOrder {
		t.Fatalf("expected VERSION_OUT_OF_ORDER for gap, got %v", err)
	}
}

func mustID(t *testing.T, raw []byte) string {
	t.Helper()
	id, err := idOf(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCompatibilityRejectionClass(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "t"

	if _, _, err := r.Register(ctx, topic, schema(fID, fName)); err != nil {
		t.Fatal(err)
	}
	// Add a REQUIRED field: backward incompatible.
	_, _, err := r.Register(ctx, topic, schema(fID, fName, `{"name":"age","type":"int"}`))
	var re *Error
	if !errors.As(err, &re) || re.Class != ClassIncompatible {
		t.Fatalf("expected INCOMPATIBLE, got %v", err)
	}
	if len(re.Report.Violations) == 0 {
		t.Fatal("incompatible error must carry explainable violations")
	}
	for _, v := range re.Report.Violations {
		t.Logf("reject: version=n/a kind=%s field=%s basis=%s", v.Kind, v.Field, v.Detail)
	}

	// Malformed input is a distinct class.
	_, _, err = r.Register(ctx, topic, []byte(`{"name":"t","fields":[{"name":"x","type":"weird"}]}`))
	if ClassOf(err) != ClassMalformed {
		t.Fatalf("expected MALFORMED_SCHEMA, got %v", err)
	}
}

func TestUnknownTopicAndVersion(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	if _, err := r.Latest(ctx, "nope"); ClassOf(err) != ClassUnknownTopic {
		t.Fatalf("expected UNKNOWN_TOPIC, got %v", err)
	}
	if _, _, err := r.Register(ctx, "u", schema(fID)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetVersion(ctx, "u", 9); ClassOf(err) != ClassUnknownVersion {
		t.Fatalf("expected UNKNOWN_VERSION, got %v", err)
	}
	// Check on empty topic is trivially compatible.
	rep, err := r.CheckCompatibility(ctx, "brand-new", compat.StrategyBackward, schema(fID))
	if err != nil || !rep.Compatible {
		t.Fatalf("empty-topic check must be compatible: %+v err=%v", rep, err)
	}
}

func TestAckMonotonicAndLag(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "t"

	for _, raw := range [][]byte{schema(fID), schema(fID, fNameN), schema(fID, fNameN, fAgeD)} {
		if _, _, err := r.Register(ctx, topic, raw); err != nil {
			t.Fatal(err)
		}
	}

	// Cannot ack an unregistered version or zero.
	if err := r.Ack(ctx, topic, "c1", 9); ClassOf(err) != ClassUnknownVersion {
		t.Fatalf("expected UNKNOWN_VERSION, got %v", err)
	}
	if err := r.Ack(ctx, topic, "c1", 0); ClassOf(err) != ClassCheckpoint {
		t.Fatalf("expected CHECKPOINT_INVALID, got %v", err)
	}

	if err := r.Ack(ctx, topic, "c1", 1); err != nil {
		t.Fatal(err)
	}
	// Ack same version again: fine. Ack backwards: rejected.
	if err := r.Ack(ctx, topic, "c1", 1); err != nil {
		t.Fatalf("re-acking same version must be idempotent: %v", err)
	}
	if err := r.Ack(ctx, topic, "c1", 2); err != nil {
		t.Fatal(err)
	}
	if err := r.Ack(ctx, topic, "c1", 1); ClassOf(err) != ClassCheckpoint {
		t.Fatalf("checkpoint must not move backwards, got %v", err)
	}

	// c2 never acks anything; register it as a known consumer at v0 by acking?
	// Lagging only reports consumers with a checkpoint row, so ack c2 to v1.
	if err := r.Ack(ctx, topic, "c2", 1); err != nil {
		t.Fatal(err)
	}
	// c3 is fully caught up at latest=3.
	if err := r.Ack(ctx, topic, "c3", 3); err != nil {
		t.Fatal(err)
	}

	lag, err := r.Lagging(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]Lag{}
	for _, l := range lag {
		seen[l.Consumer] = l
		t.Logf("lag: consumer=%s acked=%d latest=%d", l.Consumer, l.AckedVersion, l.LatestVersion)
	}
	if _, ok := seen["c1"]; !ok || seen["c1"].AckedVersion != 2 {
		t.Fatalf("c1 must be lagging at 2: %+v", seen)
	}
	if _, ok := seen["c2"]; !ok || seen["c2"].AckedVersion != 1 {
		t.Fatalf("c2 must be lagging at 1: %+v", seen)
	}
	if _, ok := seen["c3"]; ok {
		t.Fatal("c3 caught up must not be reported as lagging")
	}

	acked, latest, err := r.Checkpoint(ctx, topic, "c2")
	if err != nil || acked != 1 || latest != 3 {
		t.Fatalf("checkpoint c2 wrong: acked=%d latest=%d err=%v", acked, latest, err)
	}
}
