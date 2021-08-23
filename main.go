package main

import cons_hash "github.com/akmaljalilov/consistent_hashing/cons-hash"

const (
	PARTITION_POWER = 16
	REPLICAS        = 3
	NODE_COUNT      = 256
	ZONE_COUNT      = 16
)

func main() {
	nodes := make([]map[string]int, 0)

	/*Simple.Distribution()
	fmt.Println("-------------------------------------------------------------------------------------")
	printMovedPercent()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.SimpleRing()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.RingWithVN()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.RingWithVNOptimized()
	fmt.Println("-------------------------------------------------------------------------------------")
	limitation.LimitateVN()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.Durability()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.DurabilityWithZones()
	fmt.Println("-------------------------------------------------------------------------------------")
	durability.DurabilityWithAnchors()
	fmt.Println("-------------------------------------------------------------------------------------")*/
	for len(nodes) < NODE_COUNT {
		zone := 0
		for zone < ZONE_COUNT && len(nodes) < NODE_COUNT {
			nId := len(nodes)
			nodes = append(nodes, map[string]int{"id": nId, "zone": zone})
			zone++
		}
	}
	ring := cons_hash.BuildRing(nodes, PARTITION_POWER, REPLICAS)
	cons_hash.TestRing(ring, NODE_COUNT, ZONE_COUNT)
}
