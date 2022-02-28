package com.maximilianmichels.paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

public class LearnerTest {

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
    public void testLearner() {
        new TestKit(system) {{
            final ActorRef self = getRef();
            final ActorRef learner = system.actorOf(Props.create(Learner.class, () -> new Learner(0)));

            // Learner must learn value and tell client
            learner.tell(new Messages.Accepted(42, 23, self), self);
            expectMsg(new Messages.Response(42, 23));
            learner.tell(new Messages.Accepted(43, 24, self), self);
            expectMsg(new Messages.Response(43, 24));

            // Learner should only inform the client once
            learner.tell(new Messages.Accepted(43, 24, self), self);
            expectNoMessage(Duration.ofMillis(200));

            // Learner must _not_ learn old accepted values
            learner.tell(new Messages.Accepted(41, 23, self), self);
            expectNoMessage(Duration.ofMillis(200));
        }};
    }
}
