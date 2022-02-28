package com.maximilianmichels.paxos;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

public class PaxosTest {

    private static final Duration timeout = Duration.ofSeconds(5);

    private final Random random = new Random();

    @Test
    public void testSingleProposerAcceptorLearner() throws Exception {
        Paxos paxos = new Paxos(1, 1, 1, timeout);
        paxos.startActors();

        performAndCheckRepeatedUpdates(paxos, 1000);
    }

    @Test
    public void testMultipleProposersAcceptorsLearners() throws Exception {
        Paxos paxos = new Paxos(2, 4, 5, timeout);
        paxos.startActors();

        performAndCheckRepeatedUpdates(paxos, 200);
    }

    @Test
    public void testRandomProposersAcceptorsLearners() throws Exception {
        int numProposers = randomPositiveInt(5);
        int numAcceptors = randomPositiveInt(10);
        int numLearners = randomPositiveInt(20);

        Paxos paxos = new Paxos(numProposers, numAcceptors, numLearners, timeout);
        paxos.startActors();

        performAndCheckRepeatedUpdates(paxos, 200);
    }

    private void performAndCheckRepeatedUpdates(Paxos paxos, int iterations) throws Exception {
        long lastProposalNo = -1;
        for (int i = 0; i < iterations; i++) {
            final long randomValue = random.nextLong();
            Messages.Response response = paxos.doUpdate(randomValue);
            assertThat(response.learnedValue, is(randomValue));
            assertThat(response.proposalNo, greaterThan(lastProposalNo));
            lastProposalNo = response.proposalNo;
        }
    }

    private int randomPositiveInt(int max) {
        return random.nextInt(max) + 1;
    }
}
