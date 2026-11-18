//go:build !tarpprofile

package engine

import "sync"

// The ordinary build uses sync.Mutex itself: no instrumentation or extra state.
type engineMutex = sync.Mutex
