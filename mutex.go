//go:build !tarpprofile

package tarp

import "sync"

// The ordinary build uses sync.Mutex itself: no instrumentation or extra state.
type engineMutex = sync.Mutex
