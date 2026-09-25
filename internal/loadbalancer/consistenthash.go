package loadbalancer

import (
	"hash/crc32"
	"sort"
	"strconv"

	"github.com/1morshed1/gatewayx/internal/domain"
)

// virtualNodes is how many points on the ring each upstream gets. More virtual
// nodes => smoother key distribution (less variance in how many keys land on each
// upstream), at the cost of a bigger ring. 150 is a common, well-tested value.
const virtualNodes = 150

// hashRing is a consistent-hash ring. The key property of consistent hashing:
// when an upstream is added or removed, only ~1/N of keys remap — not all of them
// (as a plain `hash % N` would). That's what makes it good for sticky sessions and
// cache-friendly routing: a topology change doesn't reshuffle every client.
type hashRing struct {
	points []uint32       // sorted hash points
	owner  map[uint32]int // point -> upstream index
}

// newHashRing builds a ring for the given upstreams. Each upstream is placed at
// `virtualNodes` positions (weighted: heavier upstreams get proportionally more).
func newHashRing(ups []domain.Upstream) *hashRing {
	r := &hashRing{owner: make(map[uint32]int)}
	for i := range ups {
		vnodes := virtualNodes
		if w := ups[i].Weight; w > 1 {
			vnodes *= w // weight scales a node's share of the ring
		}
		for v := 0; v < vnodes; v++ {
			p := crc32.ChecksumIEEE([]byte(ups[i].URL + "#" + strconv.Itoa(v)))
			r.points = append(r.points, p)
			r.owner[p] = i
		}
	}
	sort.Slice(r.points, func(a, b int) bool { return r.points[a] < r.points[b] })
	return r
}

// pick returns the index of the upstream that owns the given key: the first ring
// point clockwise from hash(key), wrapping around at the end.
func (r *hashRing) pick(key string) int {
	if len(r.points) == 0 {
		return 0
	}
	h := crc32.ChecksumIEEE([]byte(key))
	// Binary search for the first point >= h.
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0 // wrap around
	}
	return r.owner[r.points[i]]
}
