package limitation

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
)

const (
	PARTITION_POWER = 23
	NODE_COUNT      = 65536
	DATA_ID_COUNT   = 100000000
)

func LimitateVN() {
	part2Node := []uint16{}
	vNodeCount := 1 << PARTITION_POWER
	for id := 0; id < vNodeCount; id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}
	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		part := utils.GetMD5Hash(id) % uint32(vNodeCount)
		nodeId := part2Node[part]
		nodeCounts[nodeId]++
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount, minCount := utils.GetCriticElements(nodeCounts)
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %.2f%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one node, %.2f%% under\n", minCount, under)
}
