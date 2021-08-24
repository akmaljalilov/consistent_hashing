package ring

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
	"sort"
)

const (
	NODE_COUNT     = 100
	NEW_NODE_COUNT = 101
	DATA_ID_COUNT  = 10000000
	VNODE_COUNT    = 1000
)

func SimpleRing() {
	nodeRangeStarts := []int{}
	for id := 0; id < DATA_ID_COUNT; id++ {
		nodeRangeStarts = append(nodeRangeStarts, DATA_ID_COUNT/NODE_COUNT*id)
	}

	newNodeRangeStarts := []int{}
	for id := 0; id < NEW_NODE_COUNT; id++ {
		newNodeRangeStarts = append(newNodeRangeStarts, DATA_ID_COUNT/NEW_NODE_COUNT*id)
	}
	movedIds := 0
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := utils.GetMD5Hash(id)
		nodeId := sort.Search(len(nodeRangeStarts), func(i int) bool { return nodeRangeStarts[i] >= int(hsh%DATA_ID_COUNT) }) % NODE_COUNT
		newNodeId := sort.Search(len(newNodeRangeStarts), func(i int) bool { return newNodeRangeStarts[i] >= int(hsh%DATA_ID_COUNT) }) % NEW_NODE_COUNT
		if nodeId != newNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %.2f%%\n", movedIds, movedPercent)
}
