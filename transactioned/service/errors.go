package service

import "github.com/pkg/errors"

// Sentinel errors the port maps to status codes via errors.Is.
var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrUpstream = errors.New("upstream unavailable")
)
