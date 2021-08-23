package part2

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strconv"
)

const NODE_COUNT = 100
const NEW_NODE_COUNT = 101
const DATA_ID_COUNT = 1000000

func printMovedPercent() {
	movedIds := 0

	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		bi := binary.BigEndian.Uint32(hsh[0:])
		nodeID := bi % NODE_COUNT
		newNodeId := bi % NEW_NODE_COUNT
		if nodeID != newNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %v%%\n", movedIds, movedPercent)
}
