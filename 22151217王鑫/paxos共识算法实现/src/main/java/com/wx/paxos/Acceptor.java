package paxos共识算法实现.src.main.java.com.wx.paxos;

import akka.actor.ActorRef;

public class Acceptor extends Actor {

    private final ActorRef[] learners;

    private long highestProposalNo = -1;
    private long previousValue = -1;

    Acceptor(int idx, ActorRef[] learners) {
        super(idx);
        this.learners = learners;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.Prepare.class,
                        (prepare) -> {
                            if (highestProposalNo < prepare.proposalNo) {
                                sender().tell(new Messages.Promise(prepare.proposalNo, highestProposalNo, previousValue), self());
                                highestProposalNo = prepare.proposalNo;
                            } else {
                                // TODO optimization: send nack
                            }
                        })
                .match(Messages.Accept.class,
                        (accept) -> {
                            if (highestProposalNo == accept.proposalNo) {
                                final Messages.Accepted msg = new Messages.Accepted(accept.proposalNo, accept.value, accept.client);
                                sender().tell(msg, self());
                                for (ActorRef learner : learners) {
                                    learner.tell(msg, self());
                                }
                                previousValue = accept.value;
                            }
                        })
                .build();
    }
}
