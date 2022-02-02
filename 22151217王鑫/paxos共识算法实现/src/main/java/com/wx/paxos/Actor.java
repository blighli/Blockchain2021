package paxos共识算法实现.src.main.java.com.wx.paxos;

import akka.actor.AbstractActor;

abstract class Actor extends AbstractActor {

    private final String logStringPrefix;

    protected Actor(long idx) {
        this.logStringPrefix = getClass().getSimpleName() + " " + idx + " ";
    }

    void LOG(String message) {
        System.out.println(logStringPrefix + message);
    }
}
