package paxos共识算法实现.src.main.java.com.wx.paxos;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) throws Exception {

        int numProposers = 2;
        int numAcceptors = 4;
        int numLearners = 5;

        System.out.println("numProposers: " + numProposers);
        System.out.println("numAcceptors: " + numAcceptors);
        System.out.println("numLearners: " + numLearners);

        Duration timeout = Duration.ofSeconds(5);

        Paxos paxos = new Paxos(numProposers, numAcceptors, numLearners, timeout);
        paxos.startActors();

        paxos.doUpdate(42);
        paxos.doUpdate(23);
        paxos.doUpdate(5);

        paxos.shutdown();
    }
}
