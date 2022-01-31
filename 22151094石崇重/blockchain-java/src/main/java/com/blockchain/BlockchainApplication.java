package com.blockchain;

import com.blockchain.model.TransactionPool;
import com.blockchain.services.blockchain.BlockChain;
import com.blockchain.services.miner.Miner;
import com.blockchain.services.wallet.Wallet;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BlockchainApplication {

	public static void main(String[] args) {
		SpringApplication.run(BlockchainApplication.class, args);
	}

	@Bean
	BlockChain blockChain() {
		return BlockChain.newBlockchain();
	}

	@Bean
	Wallet wallet() {
		return new Wallet();
	}

	@Bean
	TransactionPool transactionPool() {
		return new TransactionPool();
	}



	@Bean
	Miner Miner(BlockChain blockChain, TransactionPool transactionPool, Wallet wallet ) {
		return new Miner(blockChain, transactionPool, wallet );
	}


}

