package registry

import (
	"errors"
	"strings"

	"github.com/h3yow/schema-evolution-registry-go/internal/compat"
)

// ErrorClass categorizes registry failures so callers (and tests) can tell
// conflict rejections, compatibility rejections and malformed input apart.
type ErrorClass string

const (
	ClassMalformed      ErrorClass = "MALFORMED_SCHEMA"
	ClassVersionExists  ErrorClass = "VERSION_EXISTS"
	ClassVersionOrder   ErrorClass = "VERSION_OUT_OF_ORDER"
	ClassIncompatible   ErrorClass = "INCOMPATIBLE"
	ClassUnknownTopic   ErrorClass = "UNKNOWN_TOPIC"
	ClassUnknownVersion ErrorClass = "UNKNOWN_VERSION"
	ClassCheckpoint     ErrorClass = "CHECKPOINT_INVALID"
	ClassStorage        ErrorClass = "STORAGE_FAILURE"
)

// Error is a classified registry error.
type Error struct {
	Class  ErrorClass
	Op     string
	Reason string
	// Report carries the explainable compatibility report for ClassIncompatible.
	Report *compat.Report
}

func (e *Error) Error() string {
	if e.Op != "" {
		return e.Op + ": " + string(e.Class) + ": " + e.Reason
	}
	return string(e.Class) + ": " + e.Reason
}

// ClassOf returns the ErrorClass of err. Wrapped/unknown storage failures map
// to ClassStorage; a nil error yields the empty string.
func ClassOf(err error) ErrorClass {
	if err == nil {
		return ""
	}
	var re *Error
	if errors.As(err, &re) {
		return re.Class
	}
	return ClassStorage
}

// summarizeViolations renders report violations as a stable, human-readable
// reason string: "<KIND> field=<f> detail=<d>; ...".
func summarizeViolations(rep *compat.Report) string {
	if rep == nil || len(rep.Violations) == 0 {
		return "schema is incompatible under " + string(rep.Strategy)
	}
	parts := make([]string, 0, len(rep.Violations))
	for _, v := range rep.Violations {
		s := string(v.Kind)
		if v.OldName != "" && v.NewName != "" {
			s += " field=" + v.OldName + "->" + v.NewName
		} else if v.Field != "" {
			s += " field=" + v.Field
		}
		if v.OldType != "" || v.NewType != "" {
			s += " type=" + v.OldType + "->" + v.NewType
		}
		if v.Detail != "" {
			s += " (" + v.Detail + ")"
		}
		parts = append(parts, s)
	}
	return "rejected under " + string(rep.Strategy) + ": " + strings.Join(parts, "; ")
}
