package utils

import (
	"hash/fnv"
	"strconv"
)

func GetCriticElements(counts []int) (int, int) {
	maxCount := counts[0]
	minCount := counts[0]
	for _, d := range counts {
		if maxCount < d {
			maxCount = d
		} else if minCount > d {
			minCount = d
		}
	}
	return maxCount, minCount
}

func GetMD5Hash(id int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(strconv.Itoa(id)))
	return h.Sum32()
}

//                           1
//
//                    3               2
//
//                            4
//v
//

func BisectLeft(starts []int, hsh uint32) int {
	i := 0
	low, high := 0, len(starts)
	for low < high {
		i = (high + low) / 2
		if uint32(starts[i]) > hsh {
			high = i
		} else if low < i {
			low = i
		} else {
			return i
		}
	}
	return i
}
