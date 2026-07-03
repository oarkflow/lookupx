package pkg

import "testing"

func TestBitmapPromotionIncludesIncomingDocID(t *testing.T) {
	b := NewBitmap()
	// Force sparse -> dense promotion with monotonically increasing IDs.
	for i := 1; i <= sparseBitmapLimit; i++ {
		b.Add(DocID(i * 64))
	}
	incoming := DocID((sparseBitmapLimit + 1) * 64)
	b.Add(incoming)
	if !b.Has(incoming) {
		t.Fatalf("incoming doc id %d was not present after promotion", incoming)
	}
}

func TestBitmapPromotionUsesLargestSparseIDWhenOutOfOrder(t *testing.T) {
	b := NewBitmap()
	large := DocID((sparseBitmapLimit + 10) * 64)
	b.Add(large)
	for i := 1; i <= sparseBitmapLimit; i++ {
		b.Add(DocID(i))
	}
	b.Add(DocID(2)) // triggers promotion with an incoming ID smaller than max sparse ID
	if !b.Has(large) {
		t.Fatalf("largest sparse doc id %d was not present after promotion", large)
	}
}
