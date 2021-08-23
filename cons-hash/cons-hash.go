package cons_hash

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/durability"
	"log"
	"strconv"
	"time"
)

type Ring struct {
	nodes     []map[string]int
	part2Node []uint16
	replicas  int
	partShift int
}

func BuildRing(nodes []map[string]int, partPow, replicas int) *Ring {
	begin := time.Now()
	part2node := []uint16{}
	for part := 0; part < 2<<partPow; part++ {
		part2node = append(part2node, uint16(part%len(nodes)))
	}
	durability.Shuffle(part2node)
	ring := NewRing(nodes, part2node, replicas)
	fmt.Printf("%vs to build ring", time.Now().Sub(begin).Seconds())
	return ring
}

func TestRing(ring *Ring, nc, zc int) {
	begin := time.Now()
	dataIdCount := 10000000
	nodeCounts := make([]int, nc)
	zoneCounts := make([]int, zc)
	for id := 0; id < dataIdCount; id++ {
		for _, node := range ring.getNodes(id) {
			nodeCounts[node["id"]]++
			zoneCounts[node["zone"]]++
		}
	}
	fmt.Printf("%vs to build ring", time.Now().Sub(begin).Seconds())
	desiredCount := dataIdCount / len(ring.nodes) * ring.replicas
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount := nodeCounts[0]
	minCount := nodeCounts[0]
	for _, d := range nodeCounts {
		if maxCount < d {
			maxCount = d
		} else if minCount > d {
			minCount = d
		}
	}
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %v%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one noe, %v%% under\n", minCount, under)

	set := make(map[int]bool)
	for _, n := range ring.nodes {
		set[n["zone"]] = true
	}
	zoneCount := len(set)
	desiredCount = dataIdCount / zoneCount * ring.replicas
	fmt.Printf("%d: Desired data ids per zone\n", desiredCount)
	maxCount = zoneCounts[0]
	minCount = zoneCounts[0]
	for _, d := range zoneCounts {
		if maxCount < d {
			maxCount = d
		} else if minCount > d {
			minCount = d
		}
	}
	over = float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one zone, %v%% over\n", maxCount, over)
	under = float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one zone, %v%% under\n", minCount, under)
}
func NewRing(nodes []map[string]int, part2node []uint16, replicas int) *Ring {
	ring := &Ring{nodes: nodes, part2Node: part2node, replicas: replicas}
	partPow := 1
	for 2<<partPow < len(part2node) {
		partPow++
	}
	if len(part2node) != 2<<partPow {
		log.Fatal("part2node's length is not an exact power of 2")
	}
	ring.partShift = 32 - partPow
	return ring
}

func (r *Ring) getNodes(id int) []map[string]int {
	hsh := md5.Sum([]byte(strconv.Itoa(id)))
	part := binary.BigEndian.Uint32(hsh[0:]) >> r.partShift
	nodeIds := []uint16{r.part2Node[part]}
	zones := make([]map[string]int, 1)
	zones[0] = r.nodes[nodeIds[0]]
	for rep := 1; rep < r.replicas; rep++ {
		for _, n := range nodeIds {
			for _, z := range zones {
				if r.part2Node[part] == n && r.nodes[r.part2Node[part]]["id"] == z["id"] {
					part++
					if part >= uint32(len(r.part2Node)) {
						part = 0
					}
				}
			}
		}
		nodeIds = append(nodeIds, r.part2Node[part])
		zones = append(zones, r.nodes[nodeIds[len(nodeIds)-1]])
	}
	res := make([]map[string]int, len(nodeIds))
	for i, n := range nodeIds {
		res[i] = r.nodes[n]
	}
	return res
}
