package main

import (
	"fmt"
	"github.com/akmaljalilov/consistent_hashing/Simple"
	exam_simple_modulus "github.com/akmaljalilov/consistent_hashing/exam-simple-modulus"
	"github.com/akmaljalilov/consistent_hashing/ring"
)

const (
	PARTITION_POWER = 16
	REPLICAS        = 3
	NODE_COUNT      = 256
	ZONE_COUNT      = 16
)

func main() {
	Simple.SimpleModulus()
	fmt.Println("-------------------------------------------------------------------------------------")
	exam_simple_modulus.DefineMovedPercent()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.SimpleRing()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.RingWithVN()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.RingWithVNOptimized()
	fmt.Println("-------------------------------------------------------------------------------------")
	/*limitation.LimitateVN()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.Durability()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.DurabilityWithZones()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.DurabilityWithAnchors()
	fmt.Println("-------------------------------------------------------------------------------------")
	nodes := make([]map[string]int, 0)
	for len(nodes) < NODE_COUNT {
		zone := 0
		for zone < ZONE_COUNT && len(nodes) < NODE_COUNT {
			nId := len(nodes)
			nodes = append(nodes, map[string]int{"id": nId, "zone": zone})
			zone++
		}
	}
	ring := cons_hash.BuildRing(nodes, PARTITION_POWER, REPLICAS)
	cons_hash.TestRing(ring, NODE_COUNT, ZONE_COUNT)*/
}
