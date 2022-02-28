package com.blockchain.model;

import lombok.Data;

import java.util.*;

@Data
public class TransactionPool {
    private LinkedHashMap<String, Transaction> transactions;

    public TransactionPool() {
        this.transactions = new LinkedHashMap<>();
    }


    public void clear() {
        transactions.clear();
    }


    public void updateOrAddTransaction(Transaction transaction) {
        transactions.put(transaction.getTransactionId(), transaction);
    }


    public Transaction existingTransaction(String transactionId) {
        for(Map.Entry<String, Transaction> entry : transactions.entrySet()) {
            if(entry.getKey().equals(transactionId)) {
                return entry.getValue();
            }
        }
        return null;
    }


    public ArrayList<Transaction> getTransactionList(int num) {
        ArrayList<Transaction> res = new ArrayList<>(num);
        int i = 0;
        try {
            Iterator<Map.Entry<String, Transaction>> itr = transactions.entrySet().iterator();
            while(itr.hasNext()) {
                Map.Entry<String, Transaction> entry = itr.next();
                Transaction cur = entry.getValue();
                if (i < num && cur.verifyTransaction()) {
                    res.add(cur);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }



    public void updateTransactionPool(List<Transaction> validTransactions) {
        for(Transaction transaction : validTransactions) {
            if(this.transactions.containsKey(transaction.getTransactionId())) {
                this.transactions.remove(transaction.getTransactionId());
            }
        }
    }
}
