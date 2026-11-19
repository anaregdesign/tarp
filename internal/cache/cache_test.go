package cache

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

func TestExactLRUAgainstModel(t *testing.T) {
	const limit = 3 * (MetadataBytes + 1 + 8)
	c := New(limit)
	model := []string{}
	var evictions uint64
	values := make(map[string][]byte)
	rng := rand.New(rand.NewPCG(1, 2))
	for step := 0; step < 3000; step++ {
		k := strconv.Itoa(rng.IntN(10))
		remove := func(k string) {
			if i := slices.Index(model, k); i >= 0 {
				model = slices.Delete(model, i, i+1)
			}
		}
		switch rng.IntN(4) {
		case 0:
			v := make([]byte, rng.IntN(9))
			c.Put(k, v)
			remove(k)
			model = append([]string{k}, model...)
			values[k] = v
			for {
				n := 0
				for _, key := range model {
					n += MetadataBytes + len(key) + len(values[key])
				}
				if n <= limit {
					break
				}
				delete(values, model[len(model)-1])
				model = model[:len(model)-1]
				evictions++
			}
		case 1:
			_, got := c.Get([]byte(k))
			_, want := values[k]
			if got != want {
				t.Fatalf("get %s: %t != %t", k, got, want)
			}
			if want {
				remove(k)
				model = append([]string{k}, model...)
			}
		case 2:
			c.Delete(k)
			remove(k)
			delete(values, k)
		case 3:
			got, found := c.Peek([]byte(k))
			want, exists := values[k]
			if found != exists || !slices.Equal(got, want) {
				t.Fatalf("peek %s: (%v, %t), want (%v, %t)", k, got, found, want, exists)
			}
		}
		order := []string{}
		var charge int64
		for e := c.head; e != nil; e = e.next {
			order = append(order, e.key)
			charge += int64(MetadataBytes + len(e.key) + len(e.value))
		}
		if !slices.Equal(model, order) || charge != c.used || c.used > limit {
			t.Fatalf("step %d: order=%v model=%v charge=%d used=%d", step, order, model, charge, c.used)
		}
		if c.Evictions() != evictions {
			t.Fatalf("step %d: evictions=%d want %d", step, c.Evictions(), evictions)
		}
	}
	c.Clear()
	if c.Bytes() != 0 || c.Evictions() != evictions {
		t.Fatalf("after clear: used=%d evictions=%d want 0, %d", c.Bytes(), c.Evictions(), evictions)
	}
}

func TestEvictionCounterSaturates(t *testing.T) {
	c := New(MetadataBytes + 1)
	c.evictions = math.MaxUint64 - 1
	for _, key := range []string{"a", "b", "c"} {
		c.Put(key, nil)
	}
	if c.Evictions() != math.MaxUint64 {
		t.Fatalf("eviction counter wrapped to %d", c.Evictions())
	}
}
