package Simple

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	NODE_COUNT    = 100
	DATA_ID_COUNT = 10000000
)

func Distribution() {
	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		bi := binary.BigEndian.Uint32(hsh[0:])
		nodeId := bi % NODE_COUNT
		nodeCounts[nodeId] += 1
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT
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
	fmt.Printf("%d: Least data ids on one node, %v%% under\n", minCount, under)
}
