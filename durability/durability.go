package durability

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/utils"
	"math/rand"
	"time"
)

const (
	REPLICAS        = 3
	PARTITION_POWER = 16
	VNODE_COUNT     = 1 << PARTITION_POWER
	NODE_COUNT      = 256
	DATA_ID_COUNT   = 10000000
)

func Durability() {
	part2Node := []uint16{}
	for id := 0; id < VNODE_COUNT; id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}
	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		part := utils.GetMD5Hash(id) % VNODE_COUNT
		nodeIds := []uint16{part2Node[part]}
		nodeCounts[nodeIds[0]]++
		for replica := 1; replica < REPLICAS; replica++ {
			for _, n := range nodeIds {
				if part2Node[part] == n {
					part = (part + 1) % VNODE_COUNT
				}
			}
			nodeIds = append(nodeIds, part2Node[part])
			nodeCounts[nodeIds[len(nodeIds)-1]]++
		}
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount, minCount := utils.GetCriticElements(nodeCounts)
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %.2f%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one node, %.2f%% under\n", minCount, under)
}

const ZONE_COUNT = 16

func DurabilityWithZones() {
	node2Zone := []int{}
	for len(node2Zone) < NODE_COUNT {
		zone := 0
		for zone < ZONE_COUNT && len(node2Zone) < NODE_COUNT {
			node2Zone = append(node2Zone, zone)
			zone++
		}
	}
	part2Node := []uint16{}
	for id := 0; id < VNODE_COUNT; id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}
	Shuffle(part2Node)
	nodeCounts := make([]int, NODE_COUNT)
	zoneCounts := make([]int, ZONE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		part := utils.GetMD5Hash(id) % VNODE_COUNT
		nodeIds := []uint16{part2Node[part]}
		zones := []int{node2Zone[nodeIds[0]]}
		nodeCounts[nodeIds[0]]++
		zoneCounts[zones[0]]++
		for replica := 1; replica < REPLICAS; replica++ {
			for _, n := range nodeIds {
				for _, z := range zones {
					if part2Node[part] == n && node2Zone[part2Node[part]] == z {
						part = (part + 1) % VNODE_COUNT
					}
				}
			}
			nodeIds = append(nodeIds, part2Node[part])
			zones = append(zones, node2Zone[nodeIds[len(nodeIds)-1]])
			nodeCounts[nodeIds[len(nodeIds)-1]]++
			zoneCounts[zones[len(zones)-1]]++
		}
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount, minCount := utils.GetCriticElements(nodeCounts)
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %.2f%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one node, %.2f%% under\n", minCount, under)
	desiredCount = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per zone\n", desiredCount)
	maxCount, minCount = utils.GetCriticElements(zoneCounts)
	over = float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one zone, %.2f%% over\n", maxCount, over)
	under = float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one zone, %.2f%% under\n", minCount, under)
}

func DurabilityWithAnchors() {
	node2Zone := []int{}
	for len(node2Zone) < NODE_COUNT {
		zone := 0
		for zone < ZONE_COUNT && len(node2Zone) < NODE_COUNT {
			node2Zone = append(node2Zone, zone)
			zone++
		}
	}
	hash2Index := []int{}
	index2Node := []int{}
	for id := 0; id < NODE_COUNT; id++ {
		for vId := 0; vId < VNODE_COUNT; vId++ {
			hsh := utils.GetMD5Hash(id)
			index := utils.BisectLeft(hash2Index, hsh)
			if index > len(hash2Index) || index < 0 {
				index = 0
			}
			tmp := hash2Index[0:index]
			tmp = append(tmp, int(hsh))
			hash2Index = append(tmp, hash2Index[index:]...)
			tmp = index2Node[0:index]
			tmp = append(tmp, id)
			index2Node = append(tmp, index2Node[index:]...)
		}
	}
	nodeCounts := make([]int, NODE_COUNT)
	zoneCounts := make([]int, ZONE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := utils.GetMD5Hash(id)
		index := utils.BisectLeft(hash2Index, hsh)
		if index >= len(hash2Index) || index < 0 {
			index = 0
		}
		nodeIds := []int{index2Node[index]}
		zones := []int{node2Zone[nodeIds[0]]}
		nodeCounts[nodeIds[0]]++
		zoneCounts[zones[0]]++
		for replica := 1; replica < REPLICAS; replica++ {
			for _, n := range nodeIds {
				for _, z := range zones {
					if index2Node[index] == n && node2Zone[index2Node[index]] == z {
						index = (index + 1) % VNODE_COUNT
					}
				}

			}
			nodeIds = append(nodeIds, index2Node[index])
			zones = append(zones, node2Zone[nodeIds[len(nodeIds)-1]])
			nodeCounts[nodeIds[len(nodeIds)-1]]++
			zoneCounts[zones[len(zones)-1]]++
		}
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per node\n", desiredCount)
	maxCount, minCount := utils.GetCriticElements(nodeCounts)
	over := float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one node, %.2f%% over\n", maxCount, over)
	under := float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one node, %.2f%% under\n", minCount, under)
	desiredCount = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per zone\n", desiredCount)
	maxCount, minCount = utils.GetCriticElements(zoneCounts)
	over = float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one zone, %.2f%% over\n", maxCount, over)
	under = float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one zone, %.2f%% under\n", minCount, under)
}

func Shuffle(vals []uint16) []uint16 {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	ret := make([]uint16, len(vals))
	perm := r.Perm(len(vals))
	for i, randIndex := range perm {
		ret[i] = vals[randIndex]
	}
	return ret
}
