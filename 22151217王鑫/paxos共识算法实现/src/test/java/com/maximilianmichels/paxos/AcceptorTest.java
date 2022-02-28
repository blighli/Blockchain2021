package com.maximilianmichels.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

public class AcceptorTest {

    private static ActorSystem system;

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
    public void testAcceptor() {
        new TestKit(system) {{
            final ActorRef self = getRef();
            final ActorRef[] learners = new ActorRef[]{self, self};
            final ActorRef acceptor = system.actorOf(Props.create(Acceptor.class, () -> new Acceptor(0, learners)));

            // Acceptor must promise
            acceptor.tell(new Messages.Prepare(42), self);
            expectMsg(new Messages.Promise(42, -1, -1));

            // Acceptor must ignore old proposals
            acceptor.tell(new Messages.Prepare(40), self);
            expectNoMessage(Duration.ofMillis(200));

            // Acceptor must accept
            acceptor.tell(new Messages.Accept(42, 3, self), self);
            expectMsg(new Messages.Accepted(42, 3, self));
            expectMsg(new Messages.Accepted(42, 3, self));
            expectMsg(new Messages.Accepted(42, 3, self));

            // Acceptor must _not_ accept other proposals
            acceptor.tell(new Messages.Accept(666, 3, self), self);
            expectNoMessage(Duration.ofMillis(200));
        }};
    }
}
