package com.blockchain.controller;

import com.blockchain.utils.Configuration;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.blockchain.model.Block;
import com.blockchain.model.Transaction;
import com.blockchain.model.TransactionPool;
import com.blockchain.services.blockchain.BlockChain;
import com.blockchain.services.miner.Miner;
import com.blockchain.services.wallet.Wallet;
import com.blockchain.utils.BlockChainUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

@Controller
public class BlockChainController {

    @Autowired
    private BlockChain blockChain;

    @Autowired
    private Wallet wallet;



    @Autowired
    private Miner miner;

    @Autowired
    TransactionPool transactionPool;

    @Autowired
    public BlockChainController(BlockChain blockChain, Wallet wallet,  Miner miner, TransactionPool transactionPool) {
        this.blockChain = blockChain;
        this.wallet = wallet;
        this.miner = miner;
        this.transactionPool = transactionPool;
    }


    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String welcome() {
        return "index";
    }


    @RequestMapping(value = "/blocks", method = RequestMethod.GET)
    @ResponseBody
    public List<Block> showBlocks() {

        return blockChain.getChain();
    }


    @RequestMapping(value = "/public-key", method = RequestMethod.GET)
    @ResponseBody
    public String[] showAddress(HttpServletResponse response) {
        response.addHeader("Access-Control-Allow-Origin","*");
        return BlockChainUtils.convertKeytoString(wallet.getPublicKey());
    }


    @RequestMapping(value = "/wallet", method = RequestMethod.GET)
//  @ResponseBody
    public String showBalance(Model model) {
        double balance = wallet.getBalance();
        model.addAttribute("balance", balance);

        return "wallet";
    }



    @RequestMapping(value = "/transactions", method = RequestMethod.GET)
    @ResponseBody
    public Map<String, Transaction> showTransactionPool() {
        return transactionPool.getTransactions();
    }


    @RequestMapping(value = "/transact", method = RequestMethod.POST)
    @ResponseBody
    public RedirectView launchTransaction(@RequestBody  String payloadString, HttpServletResponse response) {

        Map<String, Object> payload = new HashMap<String, Object>();
        try {

            ObjectMapper mapper = new ObjectMapper();

            payload = mapper.readValue(payloadString, new TypeReference<Map<String, Object>>() {
            });

            System.out.println(payload);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
            System.out.println("Enter error!!! msg");

        } catch (JsonMappingException e) {
            e.printStackTrace();
            System.out.println("Enter error!!! msg1");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Enter error!!! msg2");
        }


        Float amount = Float.parseFloat((String)payload.get("amount"));
        System.out.println(amount);
        String middle = payload.get("recipient").toString();
        String[] recipient = { middle.substring(1, middle.indexOf(',')),
                middle.substring(middle.indexOf(',') + 2, middle.length() - 1)};
        Transaction newTransaction = wallet.createTransaction(recipient, amount, transactionPool);
        if(newTransaction == null) {
            response.setStatus(500);
            return new RedirectView("/blocks");
        }
        return new RedirectView("/blocks");
    }


    @RequestMapping(value = "/startmine", method = RequestMethod.GET)
    @ResponseBody
    public RedirectView startMineBlock() {
        miner.startMine();
        return new RedirectView("/");
    }

    @RequestMapping(value = "/stopmine", method = RequestMethod.GET)
    @ResponseBody
    public RedirectView stopMineBlock() {

        miner.stopMine();
        return new RedirectView("/");
    }



}
