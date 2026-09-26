package registry

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/h3yow/schema-evolution-registry-go/internal/compat"
)

// TestConcurrentRegisterDeterministic drives many goroutines that register
// either the identical document or distinct compatible documents. The final
// state must be deterministic:
//   - one unique content per version, no two contents share a version;
//   - every goroutine submitting the same content gets the same version;
//   - versions are exactly 1..N with no gaps or duplicates;
//   - the identical-content fan-out has exactly one writer and N-1 idempotent
//     replays.
func TestConcurrentRegisterDeterministic(t *testing.T) {
	r, path := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "events"

	// Distinct, backward-compatible schemas: each adds one field with a
	// default, so any ordering of admission remains compatible.
	nDistinct := 20
	distinct := make([][]byte, 0, nDistinct)
	for i := 0; i < nDistinct; i++ {
		fields := []string{fID}
		for j := 0; j <= i; j++ {
			fields = append(fields, fmt.Sprintf(`{"name":"f%d","type":"string","default":""}`, j))
		}
		distinct = append(distinct, schema(fields...))
	}

	type result struct {
		raw     []byte
		version int
		existed bool
		err     error
	}
	var mu sync.Mutex
	results := make([]result, 0)
	var wg sync.WaitGroup

	launch := func(raw []byte) {
		defer wg.Done()
		v, existed, err := r.Register(ctx, topic, raw)
		mu.Lock()
		results = append(results, result{raw, v, existed, err})
		mu.Unlock()
	}

	// Fan-out: 40 goroutines all submit the SAME first document.
	const sameFanout = 40
	sameDoc := distinct[0]
	for i := 0; i < sameFanout; i++ {
		wg.Add(1)
		go launch(sameDoc)
	}
	// Every other distinct schema is submitted twice, racing each other and
	// the fan-out above. distinct[0] is already covered by the fan-out.
	for i := 1; i < len(distinct); i++ {
		raw := distinct[i]
		wg.Add(2)
		go launch(raw)
		go launch(raw)
	}
	wg.Wait()

	if len(results) != sameFanout+2*(nDistinct-1) {
		t.Fatalf("lost results: %d", len(results))
	}

	idByVersion := map[int]string{}
	versionsByContent := map[string]map[int]bool{}
	idempotentSame := 0
	writtenSame := 0

	for _, res := range results {
		if res.err != nil {
			if ClassOf(res.err) == ClassIncompatible {
				// Racing admits distinct schemas in any order; because each is
				// an additive-with-default evolution they should all be
				// compatible regardless of order, so reject here.
				t.Fatalf("distinct additive schemas raced incompatibly: %v", res.err)
			}
			t.Fatalf("unexpected registration error class=%s: %v", ClassOf(res.err), res.err)
		}
		id, err := idOf(res.raw)
		if err != nil {
			t.Fatal(err)
		}

		if existing, ok := idByVersion[res.version]; ok && existing != id {
			t.Fatalf("version %d shared by two distinct contents: %s vs %s", res.version, existing, id)
		}
		idByVersion[res.version] = id

		if versionsByContent[id] == nil {
			versionsByContent[id] = map[int]bool{}
		}
		versionsByContent[id][res.version] = true

		if string(res.raw) == string(sameDoc) {
			if res.existed {
				idempotentSame++
			} else {
				writtenSame++
			}
		}
	}

	// The fan-out document: exactly one writer.
	if writtenSame != 1 || idempotentSame != sameFanout-1 {
		t.Fatalf("same-content fan-out must have 1 writer and %d replays, got writers=%d replays=%d",
			sameFanout-1, writtenSame, idempotentSame)
	}

	// Every content maps to exactly one version.
	for id, vs := range versionsByContent {
		if len(vs) != 1 {
			t.Fatalf("content %s mapped to multiple versions: %v", id, vs)
		}
	}

	// Versions on disk must be a contiguous 1..N sequence, one record each.
	versions, err := r.Versions(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != nDistinct {
		t.Fatalf("expected %d versions, got %d", nDistinct, len(versions))
	}
	seenIDs := map[string]bool{}
	for i, rv := range versions {
		if rv.Version != i+1 {
			t.Fatalf("version sequence gap: index %d has version %d", i, rv.Version)
		}
		if seenIDs[rv.SchemaID] {
			t.Fatalf("duplicate schema id %s at version %d", rv.SchemaID, rv.Version)
		}
		seenIDs[rv.SchemaID] = true
		t.Logf("concurrent register settled: version=%d schema_id=%s", rv.Version, rv.SchemaID[:12])
	}
	t.Logf("state file: %s", path)
}

// TestConcurrentExplicitVersionConflict races two different contents at the
// same explicit version: exactly one wins; the loser gets VERSION_EXISTS; the
// published content can never be silently overwritten.
func TestConcurrentExplicitVersionConflict(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "t"

	docA := schema(fID)
	docB := schema(fNameN) // different content

	const racers = 30
	var wg sync.WaitGroup
	classes := map[ErrorClass]int{}
	var mu sync.Mutex
	for i := 0; i < racers; i++ {
		wg.Add(1)
		doc := docA
		if i%2 == 0 {
			doc = docB
		}
		go func(raw []byte) {
			defer wg.Done()
			err := r.RegisterVersion(ctx, topic, 1, raw)
			c := ClassOf(err)
			mu.Lock()
			classes[c]++
			mu.Unlock()
		}(doc)
	}
	wg.Wait()

	idA, _ := idOf(docA)
	idB, _ := idOf(docB)
	got, err := r.GetVersion(ctx, topic, 1)
	if err != nil {
		t.Fatal(err)
	}
	winner := got.SchemaID
	if winner != idA && winner != idB {
		t.Fatalf("unexpected winning content: %s", winner)
	}
	// Exactly one nil (winner); all other racers that carried the loser
	// content get VERSION_EXISTS. Racers that happened to carry the same
	// content as the winner are idempotent nils.
	if classes[""] == 0 {
		t.Fatalf("expected at least one winning nil error, classes=%v", classes)
	}
	if classes[ClassVersionExists] == 0 {
		t.Fatalf("expected VERSION_EXISTS losers, classes=%v", classes)
	}
	t.Logf("explicit-version race: winner=%s classes=nil:%d VERSION_EXISTS:%d",
		winner[:12], classes[""], classes[ClassVersionExists])

	// The persisted record must equal the winner forever.
	if again, _ := r.GetVersion(ctx, topic, 1); again.SchemaID != winner {
		t.Fatal("published version was overwritten after the race")
	}
}

// TestRestartRecovery verifies that versions and consumer checkpoints survive
// a process restart (simulated by reopening the same state file) byte-for-byte,
// and that checkpoints do not roll back.
func TestRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	ctx := context.Background()
	topic := "orders"

	first := func() {
		r, err := Open(ctx, path, WithStrategy(compat.StrategyBackward))
		if err != nil {
			t.Fatal(err)
		}
		docs := [][]byte{
			schema(fID),
			schema(fID, fNameN),
			schema(fID, fNameN, fAgeD),
		}
		for i, raw := range docs {
			v, existed, err := r.Register(ctx, topic, raw)
			if err != nil {
				t.Fatalf("register doc %d: %v", i, err)
			}
			t.Logf("before restart: registered version=%d existed=%v", v, existed)
		}
		if err := r.Ack(ctx, topic, "worker-1", 2); err != nil {
			t.Fatal(err)
		}
		if err := r.Ack(ctx, topic, "worker-2", 3); err != nil {
			t.Fatal(err)
		}
		acked, latest, _ := r.Checkpoint(ctx, topic, "worker-1")
		t.Logf("before restart: worker-1 acked=%d latest=%d", acked, latest)
	}
	first()

	// Simulate restart with a brand-new Registry over the same file.
	r2, err := Open(ctx, path, WithStrategy(compat.StrategyBackward))
	if err != nil {
		t.Fatal(err)
	}
	versions, err := r2.Versions(ctx, topic)
	if err != nil || len(versions) != 3 {
		t.Fatalf("after restart: expected 3 versions, got %d err=%v", len(versions), err)
	}
	for _, v := range versions {
		t.Logf("after restart: recovered version=%d schema_id=%s", v.Version, v.SchemaID[:12])
	}
	acked, latest, err := r2.Checkpoint(ctx, topic, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if acked != 2 || latest != 3 {
		t.Fatalf("after restart checkpoint lost or rolled back: acked=%d latest=%d", acked, latest)
	}
	t.Logf("after restart: worker-1 acked=%d latest=%d (lagging=%v)", acked, latest, acked < latest)

	lag, err := r2.Lagging(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range lag {
		t.Logf("after restart: lagging consumer=%s acked=%d latest=%d", l.Consumer, l.AckedVersion, l.LatestVersion)
		if l.Consumer == "worker-1" && l.AckedVersion == 2 && l.LatestVersion == 3 {
			found = true
		}
	}
	if !found {
		t.Fatal("worker-1 must still be reported lagging at 2 after restart")
	}

	// Checkpoint cannot roll back even across the restart boundary.
	err = r2.Ack(ctx, topic, "worker-1", 1)
	if ClassOf(err) != ClassCheckpoint {
		t.Fatalf("cross-restart rollback must be rejected, got %v", err)
	}
	t.Logf("rollback rejected across restart: class=%s reason=%v", ClassOf(err), err)

	// New registrations continue at the correct monotonic next version.
	v, existed, err := r2.Register(ctx, topic, schema(fID, fNameN, fAgeD, `{"name":"extra","type":"bool","default":false}`))
	if err != nil || v != 4 || existed {
		t.Fatalf("post-restart register must continue at version 4: v=%d existed=%v err=%v", v, existed, err)
	}
	t.Logf("after restart: next registered version=%d", v)
}

// TestIncompatibleRegisterLeavesNoPartialState ensures a rejected evolution
// changes neither versions nor checkpoints.
func TestIncompatibleRegisterLeavesNoPartialState(t *testing.T) {
	r, _ := openTestRegistry(t, compat.StrategyBackward)
	ctx := context.Background()
	topic := "t"

	if _, _, err := r.Register(ctx, topic, schema(fID, fName)); err != nil {
		t.Fatal(err)
	}
	if err := r.Ack(ctx, topic, "c", 1); err != nil {
		t.Fatal(err)
	}

	bad := schema(fID, `{"name":"renamed","type":"string","aliases":["name"]}`)
	rep, err := r.CheckCompatibility(ctx, topic, compat.StrategyBackward, bad)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pre-register check: compatible=%v", rep.Compatible)
	for _, f := range rep.Findings {
		t.Logf("  finding: kind=%s field=%s %s", f.Kind, f.Field, f.Detail)
	}
	// alias rename is backward compatible; instead use a required add.
	_, _, err = r.Register(ctx, topic, schema(fID, fName, `{"name":"req","type":"int"}`))
	if ClassOf(err) != ClassIncompatible {
		t.Fatalf("expected INCOMPATIBLE, got %v", err)
	}

	versions, _ := r.Versions(ctx, topic)
	if len(versions) != 1 {
		t.Fatalf("rejected register must leave versions untouched, got %d", len(versions))
	}
	acked, latest, _ := r.Checkpoint(ctx, topic, "c")
	if acked != 1 || latest != 1 {
		t.Fatalf("rejected register must leave checkpoints untouched: acked=%d latest=%d", acked, latest)
	}
}
