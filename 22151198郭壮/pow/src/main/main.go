package main

import (
	"crypto/sha256"
	"fmt"
	"log"
	"pow/src/hash256"
	"pow/src/reverse"
	"strconv"
	"time"
)

var blockchain []block
var difnum uint=4

type block struct {

	//版本号
	version int64

	//上一个区块的Hash
	pre_hash string

	//本区块Hash
	Hash string

	//区块存储的交易数据（这里就简易地用字符串“交易数据n进行替代”）
	Data string

	//默克尔树根
	Merkle string

	//时间戳
	Timestamp int64

	//区块高度
	Height int

	//难度值
	DiffNum uint

	//随机数
	Nonce int64
}

//区块挖矿（通过自身递增nonce值计算hash）
func mine(data string) block {
	if len(blockchain) < 1 {
		log.Panic("还未生成创世区块！")
	}
	lastBlock := blockchain[len(blockchain)-1]

	//制造一个新的区块
	newblock := block{
		version:   1,
		pre_hash:  lastBlock.Hash,
		Hash:      "",
		Data:      data,
		Merkle:    "",
		Timestamp: time.Now().Unix(),
		Height:    lastBlock.Height + 1,
		DiffNum:   difnum,
		Nonce:     0,
	}

	Hashtarget := "1"
	for i := 0; i < int(difnum); i++ {
		Hashtarget="0"+Hashtarget
	}

	//获得可使用的默克尔树根
	newblock.Merkle = reverse.Reverse(fmt.Sprintf("%x",sha256.Sum256([]byte(newblock.Data))))
	Merkle := newblock.Merkle

	//获得可使用的版本号
	intversion := newblock.version
	currentversion := strconv.FormatInt(int64(intversion), 16)
	bytes := make([]byte, 0)
	for i := 8 - len(currentversion); i > 0; i-- {
		bytes = append(bytes, '0')
	}
	for i := 0; i < len(currentversion); i++ {
		bytes = append(bytes, currentversion[i])
	}
	version := string(bytes)
	version = reverse.Reverse(version)

	//获得可使用的前一个块的哈希值
	pre_hash := newblock.pre_hash
	pre_hash = reverse.Reverse(pre_hash)

	//获得可使用时间戳
	timestamp := strconv.FormatInt(int64(newblock.Timestamp), 16)
	timestamp = reverse.Reverse(timestamp)

	//获得可使用难度目标
	Bits := strconv.FormatInt(int64(newblock.DiffNum), 16)
	Bits = reverse.Reverse(Bits)
	var Hash string
	for {

		//获得nonce
		Nonce := strconv.FormatInt(int64(newblock.Nonce), 16)
		Nonce = reverse.Reverse(Nonce)

		//获得整个区块头
		block_head := version + pre_hash + Merkle + timestamp + Bits + Nonce
		Hash = reverse.Reverse(hash256.SHA256(hash256.SHA256(block_head)))

		//如果hash小于挖矿难度值计算的一个大数，则代表挖矿成功
		if Hash < Hashtarget {
			break
		} else {
			newblock.Nonce++ //不满足条件，则不断递增随机数，直到本区块的散列值小于指定的大数
		}
	}
	newblock.Hash = Hash
	return newblock
}
func main() {
	//制造一个创世区块
	genesisBlock := block{
		version:   1,
		pre_hash:  "0000000000000000000000000000000000000000000000000000000000000000",
		Hash:      "",
		Data:      "我是创世区块！",
		Merkle:    "",
		Timestamp: time.Now().Unix(),
		Height:    1,
		DiffNum:   difnum,
		Nonce:     0,
	}
	//获得可使用的默克尔树根

	genesisBlock.Merkle = reverse.Reverse(fmt.Sprintf("%x",sha256.Sum256([]byte(genesisBlock.Data))))
	Merkle := genesisBlock.Merkle

	//获得可使用的版本号
	intversion := genesisBlock.version
	currentversion := strconv.FormatInt(int64(intversion), 16)
	bytes := make([]byte, 0)
	for i := 8 - len(currentversion); i > 0; i-- {
		bytes = append(bytes, '0')
	}
	for i := 0; i < len(currentversion); i++ {
		bytes = append(bytes, currentversion[i])
	}
	version := string(bytes)
	version = reverse.Reverse(version)

	//获得可使用的前一个块的哈希值
	pre_hash := genesisBlock.pre_hash
	pre_hash = reverse.Reverse(pre_hash)

	//获得可使用时间戳
	timestamp := strconv.FormatInt(int64(genesisBlock.Timestamp), 16)
	timestamp = reverse.Reverse(timestamp)

	//获得可使用难度目标
	Bits := strconv.FormatInt(int64(genesisBlock.DiffNum), 16)
	Bits = reverse.Reverse(Bits)
	var Hash string

	//获得nonce
	Nonce := strconv.FormatInt(int64(genesisBlock.Nonce), 16)
	Nonce = reverse.Reverse(Nonce)

	//获得整个区块头
	block_head := version + pre_hash + Merkle + timestamp + Bits + Nonce
	Hash = reverse.Reverse(hash256.SHA256(hash256.SHA256(block_head)))

	genesisBlock.Hash = Hash
	fmt.Println(genesisBlock)
	//将创世区块添加进区块链
	blockchain = append(blockchain, genesisBlock)
	for i := 0; i < 10; i++ {
		newBlock := mine("交易数据" + strconv.Itoa(i))
		blockchain = append(blockchain, newBlock)
		fmt.Println(newBlock)
	}
}
