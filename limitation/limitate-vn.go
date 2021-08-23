package limitation

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	PARTITION_POWER = 23
	PARTITION_SHIFT = 32 - PARTITION_POWER
	NODE_COUNT      = 65536
	DATA_ID_COUNT   = 100000000
)

func LimitateVN() {
	part2Node := []uint16{}
	for id := 0; id < (2 << PARTITION_POWER); id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}
	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		part := binary.BigEndian.Uint32(hsh[0:]) >> PARTITION_SHIFT
		nodeId := part2Node[part]
		nodeCounts[nodeId]++
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
