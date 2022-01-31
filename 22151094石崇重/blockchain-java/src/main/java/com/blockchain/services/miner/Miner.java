package com.blockchain.services.miner;
import com.blockchain.model.Block;
import com.blockchain.model.Transaction;
import com.blockchain.model.TransactionPool;
import com.blockchain.services.blockchain.BlockChain;
import com.blockchain.services.wallet.Wallet;
import com.blockchain.utils.Configuration;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;

@Data
@Component
public class Miner implements Runnable {
    private BlockChain blockchain;
    private TransactionPool pool;
    private Wallet wallet;
    private boolean run;

    @Autowired
    public Miner(BlockChain blockchain, TransactionPool pool, Wallet wallet ) {
        this.blockchain = blockchain;
        this.pool = pool;
        this.wallet = wallet;
    }

    public void startMine() {
        if(run) {
            return;
        }
        run = true;
        Thread thread = new Thread(this);
        thread.start();
    }

    public void stopMine() {

        run = false;
        Block.minable = false;
    }

    /**
     * Thread Entrance, Miner try to mine block
     */
    public void run() {
        while (run) {
            ArrayList<Transaction> validTransaction = pool.getTransactionList(Configuration.TRANSACTION_NUM);
            validTransaction.add(Transaction.rewardMinner(wallet, Configuration.MINING_REWARD));
            Block latestBlock = blockchain.addBlock(validTransaction);
            if (latestBlock == null) {
                List<Block> chain = blockchain.getChain();
                pool.updateTransactionPool(chain.get(chain.size() - 1).getTransactions());
                continue;
            }
            pool.updateTransactionPool(validTransaction);
            wallet.updateUTXOsFromWholeBlockChain(blockchain);
        }
    }
}
