package exam_simple_modulus

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
)

const NODE_COUNT = 100
const NEW_NODE_COUNT = 101
const DATA_ID_COUNT = 1000000

func DefineMovedPercent() {
	movedIds := 0
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := utils.GetMD5Hash(id)
		nodeID := hsh % NODE_COUNT
		newNodeId := hsh % NEW_NODE_COUNT
		if nodeID != newNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %.2f%%\n", movedIds, movedPercent)
}
