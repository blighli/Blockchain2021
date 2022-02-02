package paxos共识算法实现.src.main.java.com.wx.paxos;

import akka.actor.ActorRef;

public class Learner extends Actor {

    private long lastProposalNo = -1;
    private long lastValue = -1;

    Learner(int idx) {
        super(idx);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.Accepted.class,
                        (accepted) -> {
                            if (lastProposalNo < accepted.proposalNo) {
                                lastProposalNo = accepted.proposalNo;
                                lastValue = accepted.value;
                                LOG("Received accepted. No: " + accepted.proposalNo + " value: " + accepted.value);
                                accepted.client.tell(new Messages.Response(accepted.proposalNo, lastValue), ActorRef.noSender());
                            }
                        })
                .build();
    }
}
