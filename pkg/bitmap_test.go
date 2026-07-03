package pkg

import "testing"

func TestBitmapPromoteReservesWordForExact64Boundary(t *testing.T) {
	b := &Bitmap{sparse: []DocID{64}}

	b.promote(64)

	if !b.Has(64) {
		t.Fatalf("expected promoted bitmap to contain doc id 64")
	}
	if len(b.words) != 2 {
		t.Fatalf("expected 2 words for doc id 64, got %d", len(b.words))
	}
}
