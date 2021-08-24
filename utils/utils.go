package utils

import (
	"crypto/md5"
	"encoding/binary"
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
	hsh := md5.Sum([]byte(strconv.Itoa(id)))
	return binary.BigEndian.Uint32(hsh[0:])
}
