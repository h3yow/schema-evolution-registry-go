// Command basic demonstrates local schema registration, deterministic
// conflicts, explainable compatibility rejections, consumer confirmations and
// restart recovery against a JSON snapshot in a temporary directory.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/h3yow/schema-evolution-registry-go/registry"
	"github.com/h3yow/schema-evolution-registry-go/schema"
)

func f(name string, k schema.Kind, optional bool) schema.Field {
	return schema.Field{Name: name, Type: k, Optional: optional}
}

func show(prefix string, err error) {
	if err == nil {
		fmt.Printf("%s: OK\n", prefix)
		return
	}
	var re *registry.Error
	if errors.As(err, &re) {
		fmt.Printf("%s: code=%s msg=%s reasons=%v\n", prefix, re.Code, re.Message, re.Reasons)
	} else {
		fmt.Printf("%s: %v\n", prefix, err)
	}
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "registry-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "registry.json")

	// --- phase 1: first process lifetime ---
	r, err := registry.New(ctx, registry.Options{Path: path, DefaultStrategy: schema.CompatBackward})
	if err != nil {
		log.Fatal(err)
	}

	v1 := schema.Schema{Fields: []schema.Field{f("id", schema.KindLong, false), f("name", schema.KindString, false)}}
	res, err := r.Register(ctx, "users", v1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[register] v%d reused=%v fp=%s\n", res.Version, res.Reused, res.Fingerprint)

	// duplicate content: deterministic reuse
	res2, _ := r.Register(ctx, "users", v1)
	fmt.Printf("[duplicate] -> v%d reused=%v\n", res2.Version, res2.Reused)

	// different content occupying v1: conflict
	_, err = r.RegisterVersion(ctx, "users", 1, schema.Schema{Fields: []schema.Field{f("id", schema.KindString, false)}})
	show("[conflict]", err)

	// incompatible evolution: required field added under BACKWARD
	bad := schema.Schema{Fields: []schema.Field{f("id", schema.KindLong, false), f("name", schema.KindString, false), f("email", schema.KindString, false)}}
	_, err = r.Register(ctx, "users", bad)
	show("[incompatible]", err)

	// compatible evolution
	good := schema.Schema{Fields: []schema.Field{f("id", schema.KindLong, false), f("name", schema.KindString, false), f("email", schema.KindString, true)}}
	res3, err := r.Register(ctx, "users", good)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[compatible] v%d published\n", res3.Version)

	if err := r.Confirm(ctx, "users", "consumer-a", 1); err != nil {
		log.Fatal(err)
	}
	show("[rollback rejected]", r.Confirm(ctx, "users", "consumer-a", 0))
	if err := r.Close(); err != nil {
		log.Fatal(err)
	}

	// --- phase 2: simulate restart ---
	r2, err := registry.New(ctx, registry.Options{Path: path, DefaultStrategy: schema.CompatBackward})
	if err != nil {
		log.Fatal(err)
	}
	pos, err := r2.GetConfirmation(ctx, "users", "consumer-a")
	show("[restored position]", err)
	fmt.Printf("[restart] consumer-a confirmed v%d; next registration continues the sequence\n", pos)

	behind, err := r2.ConsumersBehind(ctx, "users")
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range behind {
		fmt.Printf("[behind] consumer=%s confirmed=v%d latest=v%d pending=%v\n",
			v.Consumer, v.Confirmed, v.Latest, v.PendingVersions)
	}
}
