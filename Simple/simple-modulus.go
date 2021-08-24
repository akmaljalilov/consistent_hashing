package Simple

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
)

const (
	NODE_COUNT    = 100
	DATA_ID_COUNT = 10000000
)

func SimpleModulus() {
	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := utils.GetMD5Hash(id)
		nodeId := hsh % NODE_COUNT
		nodeCounts[nodeId] += 1
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount, minCount := utils.GetCriticElements(nodeCounts)
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %.2f%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one node, %.2f%% under\n", minCount, under)
}
