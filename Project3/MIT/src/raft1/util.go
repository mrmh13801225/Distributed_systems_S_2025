package raft

import (
	"log"
)

// Debugging

func DPrintf(format string, a ...any) {
	if Debug {
		log.Printf(format, a...)
	}
}

func fillSlice[T any](n int, value T) []T {
	s := make([]T, n)
	for i := range s {
		s[i] = value
	}
	return s
}
