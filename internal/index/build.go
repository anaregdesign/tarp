package index

// Mutations provides read-your-writes key operations during replay.
type Mutations interface {
	Lookup(string) (Address, bool, error)
	Put(string, Address) error
	Delete(string) error
}

// InitialBuilder optionally populates an empty index directly in replay order.
// Concurrent readers continue seeing the empty index until Finish succeeds.
type InitialBuilder interface {
	BeginBuild() (InitialBuild, error)
}

type InitialBuild interface {
	Mutations
	// Finish visits each final live key once when measure is non-nil. It
	// publishes only if every visit succeeds and always releases the builder.
	Finish(measure func(string, Address) error) error
	Abort()
}
