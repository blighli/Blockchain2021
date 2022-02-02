package com.maximilianmichels.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

public class ClientTest {

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
            final ActorRef[] proposers = new ActorRef[]{self, self};
            final ActorRef clientProxy = system.actorOf(
                    Props.create(ClientProxy.class, () -> new ClientProxy(proposers)));

            // ClientProxy must forward request to proposers
            clientProxy.tell(new Messages.Request(42), self);
            expectMsg(new Messages.Request(42));
            expectMsg(new Messages.Request(42));

            // ClientProxy should forward response to client once
            clientProxy.tell(new Messages.Response(23, 41), self);
            clientProxy.tell(new Messages.Response(23, 41), self);
            expectMsg(new Messages.Response(23, 41));
            expectNoMessage(Duration.ofMillis(200));

            // ClientProxy should ignore old responses
            clientProxy.tell(new Messages.Response(22, 41), self);
            expectNoMessage(Duration.ofMillis(200));
        }};
    }
}
