package com.blockchain.services.wallet;


import com.blockchain.model.*;
import com.blockchain.services.blockchain.BlockChain;
import com.blockchain.utils.BlockChainUtils;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.security.*;
import java.security.spec.ECGenParameterSpec;
import java.util.HashMap;
import java.util.Map;


@Data
@Component
public class Wallet {
    private PrivateKey privateKey;
    private PublicKey publicKey;


    @Autowired
    private BlockChain blockChain;

    private Map<String, TransactionOutput> UTXOs;


    public Wallet() {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        genKeyPair();
        this.UTXOs = new HashMap<>();
    }


    public float getBalance() {
        float balance = 0;
        if(this.getUTXOs().isEmpty()) return balance;
        for(TransactionOutput txOutput : this.getUTXOs().values()) {
            balance += txOutput.getAmount();
        }
        return balance;
    }


    public void updateUTXOsFromWholeBlockChain(BlockChain blockChain) {
        this.setUTXOs(blockChain.findWalletUTXOs(this.getPublicKey()));
    }


    public void updateUTXOsFromMinnedBlock(Block minnedBlock) {
        for(Transaction tx : minnedBlock.getTransactions()) {
            for(TransactionOutput txOutput : tx.getOutputs()) {
                PublicKey publicKey = BlockChainUtils.convertStringtoKey(txOutput.getRecipient());
                if(publicKey.equals(this.getPublicKey())) {
                    this.UTXOs.put(txOutput.getId(), txOutput);
                }
            }
        }
    }


    @Override
    public String toString() {
        return "Wallet -" +
                "\n publicKey: " + String.valueOf(this.getPublicKey()) +
                "\n privateKey: " + String.valueOf(this.getPrivateKey()) +
                "\n UTXOs: " + String.valueOf(this.getUTXOs()) + "\n";
    }




    private void genKeyPair() {
        try{
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC","SunEC");
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp192k1");
            keyGen.initialize(ecSpec, random);
            KeyPair keyPair = keyGen.generateKeyPair();
            privateKey = keyPair.getPrivate();
            publicKey  = keyPair.getPublic();
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public Transaction createTransaction(String[] recipient, float amount,
                                         TransactionPool transactionPool) {
        float balance = getBalance();

        if(amount > balance) {
            //TODO: handle this situation
            System.out.println("Amount exceceds current balance: " + amount + " > " + balance);
        }

        Transaction transaction = Transaction.newTransaction(this, recipient, amount);
        transactionPool.updateOrAddTransaction(transaction);

        return transaction;
    }

}
