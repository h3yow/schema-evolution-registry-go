package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Fingerprint returns the hex SHA-256 of the canonical JSON encoding of the
// schema. Two schemas with identical field names, order, types and optionality
// share a fingerprint; any content difference produces a different one.
func (s Schema) Fingerprint() string {
	// Field order is significant and preserved by encoding/json, giving a
	// stable canonical form for the already-validated flat structure.
	data, _ := json.Marshal(s)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
