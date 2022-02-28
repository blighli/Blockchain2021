package paxos共识算法实现.src.main.java.com.wx.paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

public class ClientProxy extends AbstractActor {

    private final ActorRef[] proposers;

    private ActorRef client;

    private long proposalNo;

    ClientProxy(ActorRef[] proposers) {
        this.proposers = proposers;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.Request.class,
                        (req) -> {
                            client = sender();
                            for (ActorRef proposer : proposers) {
                                proposer.tell(req, self());
                            }
                        }
                )
                .match(Messages.Response.class,
                        (response) -> {
                            // only reply to client once because multiple learners might inform us
                            if (response.proposalNo > proposalNo) {
                                client.tell(response, self());
                                proposalNo = response.proposalNo;
                            }
                        }
                )
                .build();
    }
}
