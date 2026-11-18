package cache

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

func TestExactLRUAgainstModel(t *testing.T) {
	const limit = 3 * (MetadataBytes + 1 + 8)
	c := New(limit)
	model := []string{}
	values := make(map[string][]byte)
	rng := rand.New(rand.NewPCG(1, 2))
	for step := 0; step < 3000; step++ {
		k := strconv.Itoa(rng.IntN(10))
		remove := func(k string) {
			if i := slices.Index(model, k); i >= 0 {
				model = slices.Delete(model, i, i+1)
			}
		}
		switch rng.IntN(3) {
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
	}
}
