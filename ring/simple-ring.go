package ring

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
)

const (
	NODE_COUNT     = 100
	NEW_NODE_COUNT = 101
	DATA_ID_COUNT  = 10_000_000
	VNODE_COUNT    = 1000
)

func SimpleRing() {
	nodeRangeStarts := []int{}
	p := DATA_ID_COUNT / NODE_COUNT
	for id := 0; id < NODE_COUNT; id++ {
		nodeRangeStarts = append(nodeRangeStarts, p*id)
	}

	newNodeRangeStarts := []int{}
	p = DATA_ID_COUNT / NEW_NODE_COUNT
	for id := 0; id < NEW_NODE_COUNT; id++ {
		newNodeRangeStarts = append(newNodeRangeStarts, p*id)
	}
	movedIds := 0
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := utils.GetMD5Hash(id)
		nodeId := utils.BisectLeft(nodeRangeStarts, hsh, DATA_ID_COUNT, NODE_COUNT)
		newNodeId := utils.BisectLeft(newNodeRangeStarts, hsh, DATA_ID_COUNT, NEW_NODE_COUNT)
		if nodeId != newNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %.2f%%\n", movedIds, movedPercent)
}
