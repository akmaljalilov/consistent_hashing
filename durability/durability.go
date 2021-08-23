package durability

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"time"
)

const (
	REPLICAS        = 3
	PARTITION_POWER = 16
	PARTITION_SHIFT = 32 - PARTITION_POWER
	PARTITION_MAX   = 2<<PARTITION_POWER - 1
	NODE_COUNT      = 256
	DATA_ID_COUNT   = 10000000
	ZONE_COUNT      = 16
	VNODE_COUNT     = 100
)

func Durability() {
	part2Node := []uint16{}
	for id := 0; id < (2 << PARTITION_POWER); id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}

	nodeCounts := make([]int, NODE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		part := binary.BigEndian.Uint32(hsh[0:]) >> PARTITION_SHIFT
		nodeIds := []uint16{part2Node[part]}
		nodeCounts[nodeIds[0]]++
		for replica := 1; replica < REPLICAS; replica++ {
			for _, n := range nodeIds {
				if part2Node[part] == n {
					part++
					if part > PARTITION_MAX {
						part = 0
					}
				}
			}
			nodeIds = append(nodeIds, part2Node[part])
			nodeCounts[nodeIds[len(nodeIds)-1]]++
		}
	}
	desiredCount := DATA_ID_COUNT / NODE_COUNT * REPLICAS
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
	for id := 0; id < (2 << PARTITION_POWER); id++ {
		part2Node = append(part2Node, uint16(id%NODE_COUNT))
	}
	Shuffle(part2Node)

	nodeCounts := make([]int, NODE_COUNT)
	zoneCounts := make([]int, ZONE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		part := binary.BigEndian.Uint32(hsh[0:]) >> PARTITION_SHIFT
		nodeIds := []uint16{part2Node[part]}
		zones := []int{node2Zone[nodeIds[0]]}
		nodeCounts[nodeIds[0]]++
		zoneCounts[zones[0]]++
		for replica := 1; replica < REPLICAS; replica++ {
			for _, n := range nodeIds {
				for _, z := range zones {
					if part2Node[part] == n && node2Zone[part2Node[part]] == z {
						part++
						if part > PARTITION_MAX {
							part = 0
						}
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

	desiredCount = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per zone\n", desiredCount)
	maxCount = zoneCounts[0]
	minCount = zoneCounts[0]
	for _, d := range zoneCounts {
		if maxCount < d {
			maxCount = d
		} else if minCount > d {
			minCount = d
		}
	}
	over = float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one zone, %v%% over\n", maxCount, over)
	under = float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one zone, %v%% under\n", minCount, under)
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
			hsh := md5.Sum([]byte(strconv.Itoa(id)))
			bi := binary.BigEndian.Uint32(hsh[0:])
			index := sort.Search(len(hash2Index), func(i int) bool { return hash2Index[i] >= int(bi) })
			if index > len(hash2Index) {
				index = 0
			}
			tmp := hash2Index[0:index]
			tmp = append(tmp, int(bi))
			hash2Index = append(tmp, hash2Index[index:]...)
			tmp = index2Node[0:index]
			tmp = append(tmp, id)
			index2Node = append(tmp, index2Node[index:]...)
		}
	}
	nodeCounts := make([]int, NODE_COUNT)
	zoneCounts := make([]int, ZONE_COUNT)
	for id := 0; id < DATA_ID_COUNT; id++ {
		hsh := md5.Sum([]byte(strconv.Itoa(id)))
		bi := binary.BigEndian.Uint32(hsh[0:])
		index := sort.Search(len(hash2Index), func(i int) bool { return hash2Index[i] >= int(bi) })
		if index >= len(hash2Index) {
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
						index++
						if index >= len(hash2Index) {
							index = 0
						}
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

	desiredCount = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
	fmt.Printf("%d: Desired data ids per zone\n", desiredCount)
	maxCount = zoneCounts[0]
	minCount = zoneCounts[0]
	for _, d := range zoneCounts {
		if maxCount < d {
			maxCount = d
		} else if minCount > d {
			minCount = d
		}
	}
	over = float32(100*(maxCount-desiredCount)) / float32(desiredCount)
	fmt.Printf("%d: Most data ids on one zone, %v%% over\n", maxCount, over)
	under = float32(100*(desiredCount-minCount)) / float32(desiredCount)
	fmt.Printf("%d: Least data ids on one zone, %v%% under\n", minCount, under)
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
