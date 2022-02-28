package hash256

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
)

func SHA256(text string) string {
	var hashInstance hash.Hash
	hashInstance=sha256.New()
	arr,_:=hex.DecodeString(text)
	hashInstance.Write(arr)
	bytes:=hashInstance.Sum(nil)
	return fmt.Sprintf("%x",bytes)
}