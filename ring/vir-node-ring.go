package ring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
)

func RingWithVN() {
	vNodeRangeStarts := []int{}
	vNode2Node := []int{}
	for id := 0; id < VNODE_COUNT; id++ {
		vNodeRangeStarts = append(vNodeRangeStarts, DATA_ID_COUNT/VNODE_COUNT*id)
		vNode2Node = append(vNode2Node, id%NODE_COUNT)
	}
	newVNode2Node := append([]int{}, vNode2Node...)
	newNodeId := NODE_COUNT + 1
	vNodesToReassign := VNODE_COUNT / NEW_NODE_COUNT
	for vNodesToReassign > 0 {
		for nodeToTakeFrom := 0; nodeToTakeFrom < NODE_COUNT; nodeToTakeFrom++ {
			for vNodeId, nodeId := range newVNode2Node {
				if nodeId == nodeToTakeFrom {
					newVNode2Node[vNodeId] = newNodeId
					vNodesToReassign--
					break
				}
			}
			if vNodesToReassign <= 0 {
				break
			}
		}
	}
	movedIds := 0
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		bi := binary.BigEndian.Uint32(hsh[0:])
		vNodeId := sort.Search(len(vNodeRangeStarts), func(i int) bool { return vNodeRangeStarts[i] >= int(bi%DATA_ID_COUNT) }) % VNODE_COUNT
		nodeId := vNode2Node[vNodeId]
		nNodeId := newVNode2Node[vNodeId]
		if nodeId != nNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %v%%\n", movedIds, movedPercent)
}

func RingWithVNOptimized() {

	vNode2Node := []int{}
	for id := 0; id < VNODE_COUNT; id++ {
		vNode2Node = append(vNode2Node, id%NODE_COUNT)
	}
	newVNode2Node := append([]int{}, vNode2Node...)
	newNodeId := NODE_COUNT + 1
	vNodesToReassign := VNODE_COUNT / NEW_NODE_COUNT
	for vNodesToReassign > 0 {
		for nodeToTakeFrom := 0; nodeToTakeFrom < NODE_COUNT; nodeToTakeFrom++ {
			for vNodeId, nodeId := range newVNode2Node {
				if nodeId == nodeToTakeFrom {
					newVNode2Node[vNodeId] = newNodeId
					vNodesToReassign--
					break
				}
			}
			if vNodesToReassign <= 0 {
				break
			}
		}
	}
	movedIds := 0
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		bi := binary.BigEndian.Uint32(hsh[0:])
		vNodeId := bi % VNODE_COUNT
		nodeId := vNode2Node[vNodeId]
		nNodeId := newVNode2Node[vNodeId]
		if nodeId != nNodeId {
			movedIds++
		}
	}
	movedPercent := float32(100*movedIds) / float32(DATA_ID_COUNT)
	fmt.Printf("%d ids moved, %v%%\n", movedIds, movedPercent)
}
