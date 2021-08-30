package main

import "github.com/akmaljalilov/consistent_hashing/ring"

const (
	PARTITION_POWER = 16
	REPLICAS        = 3
	NODE_COUNT      = 256
	ZONE_COUNT      = 16
)

func main() {
	/*nC := 4
	vNC := 25
	dI := 1000
	vnP := make([]int, vNC)
	vnI := make([]int, vNC)
	nI:=make([]int,nC)
	for i := 0; i < vNC; i++ {
		vnP[i] = i % nC
		nI[i%nC]++
	}
	for i := 0; i < dI; i++ {
		vnI[i%vNC]++
	}
	min, max := utils.GetCriticElements(vnI)
	fmt.Println(min)
	fmt.Println(max)
	fmt.Println(dI/vNC)
	min, max = utils.GetCriticElements(nI)
	fmt.Println(min)
	fmt.Println(max)
	fmt.Println(vNC/nC)*/

	//durability.DurabilityWithAnchors()
	//durability.Durability()
	//ring.RingWithVNOptimized()
	//Simple.SimpleModulus()
	ring.SimpleRing()
	//limitation.LimitateVN()
	/*	ring.SimpleRing()
		exam_simple_modulus.DefineMovedPercent()

	*/
	/*fmt.Println("-------------------------------------------------------------------------------------")
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.SimpleRing()
	fmt.Println("-------------------------------------------------------------------------------------")
	ring.RingWithVN()
	fmt.Println("-------------------------------------------------------------------------------------")
	fmt.Println("-------------------------------------------------------------------------------------")

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
