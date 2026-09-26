// Command registry-demo drives the local schema registry entirely against a
// temp directory, printing versions, compatibility findings, error classes and
// a simulated restart recovery. Run with:
//
//	go run ./cmd/registry-demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/h3yow/schema-evolution-registry-go/internal/compat"
	"github.com/h3yow/schema-evolution-registry-go/registry"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "registry-demo-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "registry.json")

	fmt.Println("== local schema registry demo ==")
	fmt.Println("state file:", path)

	reg, err := registry.Open(ctx, path, registry.WithStrategy(compat.StrategyFull))
	must(err)

	topic := "orders"
	schemas := [][]byte{
		[]byte(`{"name":"Order","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"}]}`),
		// Add optional field: FULL compatible.
		[]byte(`{"name":"Order","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"},{"name":"note","type":"string","default":""}]}`),
		// Adding a REQUIRED field is rejected under FULL.
		[]byte(`{"name":"Order","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"},{"name":"note","type":"string","default":""},{"name":"required_tag","type":"string"}]}`),
	}

	// v1
	v, existed, err := reg.Register(ctx, topic, schemas[0])
	must(err)
	fmt.Printf("register: topic=%s -> version=%d alreadyExisted=%v\n", topic, v, existed)

	// v2 compatible
	v, existed, err = reg.Register(ctx, topic, schemas[1])
	must(err)
	fmt.Printf("register: topic=%s -> version=%d alreadyExisted=%v\n", topic, v, existed)

	// v3 rejected under FULL
	_, _, err = reg.Register(ctx, topic, schemas[2])
	fmt.Printf("register rejected: class=%s\n  reason=%v\n", registry.ClassOf(err), err)
	if re, ok := err.(*registry.Error); ok && re.Report != nil {
		for _, f := range re.Report.Violations {
			fmt.Printf("  violation: kind=%s field=%s basis=%s\n", f.Kind, f.Field, f.Detail)
		}
	}

	// Duplicate submission is deterministic/idempotent.
	v, existed, err = reg.Register(ctx, topic, schemas[1])
	must(err)
	fmt.Printf("duplicate submit: -> version=%d alreadyExisted=%v\n", v, existed)

	// Consumer acks and lag.
	must(reg.Ack(ctx, topic, "billing", 1))
	acked, latest, _ := reg.Checkpoint(ctx, topic, "billing")
	fmt.Printf("checkpoint: consumer=billing acked=%d latest=%d\n", acked, latest)
	lag, _ := reg.Lagging(ctx, topic)
	for _, l := range lag {
		fmt.Printf("lagging: consumer=%s acked=%d latest=%d\n", l.Consumer, l.AckedVersion, l.LatestVersion)
	}

	// Simulate process restart.
	fmt.Println("-- simulating restart (reopen same file) --")
	reg2, err := registry.Open(ctx, path, registry.WithStrategy(compat.StrategyFull))
	must(err)
	versions, err := reg2.Versions(ctx, topic)
	must(err)
	for _, rv := range versions {
		fmt.Printf("recovered: version=%d schema_id=%s\n", rv.Version, rv.SchemaID[:12])
	}
	acked, latest, _ = reg2.Checkpoint(ctx, topic, "billing")
	fmt.Printf("recovered checkpoint: consumer=billing acked=%d latest=%d\n", acked, latest)

	// Rollback across restart is rejected.
	err = reg2.Ack(ctx, topic, "billing", 0)
	fmt.Printf("rollback ack: class=%s reason=%v\n", registry.ClassOf(err), err)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
