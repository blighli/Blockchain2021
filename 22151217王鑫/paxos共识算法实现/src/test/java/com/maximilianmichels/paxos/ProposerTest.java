package com.maximilianmichels.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class ProposerTest {

    static ActorSystem system;
    static final Duration timeout = Duration.ofSeconds(2);

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testProposer() {
        new TestKit(system) {{
            final ActorRef self = getRef();
            final ActorRef[] acceptors = new ActorRef[] {self, self};
            final ActorRef proposer = system.actorOf(Props.create(Proposer.class, () -> new Proposer(0, acceptors, timeout)));

            // Proposer must increase proposal numbers
            proposer.tell(new Messages.Request(23), self);
            expectMsg(new Messages.Prepare(1));
            expectMsg(new Messages.Prepare(1));
            proposer.tell(new Messages.Request(23), self);
            expectMsg(new Messages.Prepare(2));
            expectMsg(new Messages.Prepare(2));

            // Proposer must ignore old promises
            proposer.tell(new Messages.Promise(1, -1, -1), self);
            expectNoMessage(Duration.ofMillis(200));

            // Proposer must send Accept if a quorum of Acceptors send a Promise
            proposer.tell(new Messages.Promise(2, -1, -1), self);
            expectNoMessage(Duration.ofMillis(200));
            proposer.tell(new Messages.Promise(2, -1, -1), self);
            expectMsg(new Messages.Accept(2, 23, self));
            expectMsg(new Messages.Accept(2, 23, self));

            // Proposer must override its proposal number with any higher proposol numbers received by acceptors
            proposer.tell(new Messages.Promise(2, 666, 42), self);
            proposer.tell(new Messages.Request(2), self);
            expectMsg(new Messages.Prepare(667));
            expectMsg(new Messages.Prepare(667));

            // Proposer must retry in case of missing quorum after timeout
            proposer.tell(new Messages.Request(23), self);
            expectMsg(new Messages.Prepare(668));
            expectMsg(new Messages.Prepare(668));
            proposer.tell(new Messages.Request(668), self);
            expectMsg(new Messages.Prepare(669));
            expectMsg(new Messages.Prepare(669));
            expectNoMessage(Duration.ofMillis(200));
        }};
    }
}
