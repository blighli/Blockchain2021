Paxos
=====

A basic implementation of Paxos for reaching consensus with an arbitrary number
of Proposers/Acceptors/Learners.

- Checkout the [main code](src/main/java/com/wx/paxos)
    - [Proposer](src/main/java/com/wx/paxos/Proposer.java)
    - [Acceptor](src/main/java/com/wx/paxos/Acceptor.java)
    - [Learner](src/main/java/com/wx/paxos/Learner.java)

- Run the `Main` class for a demo of its functionality.

- Checkout the [tests](src/test/java/com/maximilianmichels/paxos)

Implementation
--------------

Paxos is known for being hard to implement. It depends. The basic idea
from
[Lamport's paper](http://lamport.azurewebsites.net/pubs/pubs.html#lamport-paxos)
can be expressed quite elegantly with an Actor based implementation. We use
`Akka` here. In a production system, the challenge is to scale Paxos, e.g. by avoiding
the repeated leader election phase in the prepare phase.

Build
-----

`gradle build` - builds and runs tests


