//go:build !tarpprofile

package benchmark

import "testing"

func vmProfileMeasurements(t *testing.T, _ int, _ vmMeasureWriter) {
	t.Fatal("diagnostic measurements require the explicit tarpprofile build tag")
}
