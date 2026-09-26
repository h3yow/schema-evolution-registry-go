package registry

import (
	"context"

	"github.com/h3yow/schema-evolution-registry-go/internal/schemadef"
)

func idOf(raw []byte) (string, error) {
	s, err := schemadef.Parse(context.Background(), raw)
	if err != nil {
		return "", err
	}
	return s.ID()
}
