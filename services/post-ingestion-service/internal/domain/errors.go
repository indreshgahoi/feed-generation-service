package domain

import "errors"

var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrInvalidInput    = errors.New("invalid input")
	ErrSelfFollow      = errors.New("cannot follow yourself")
	ErrRateLimited     = errors.New("rate limited: try again shortly")
	ErrContentRejected = errors.New("content rejected by moderation filter")
)
