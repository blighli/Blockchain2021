package paxos共识算法实现.src.main.java.com.wx.paxos;

import akka.actor.ActorRef;

import java.util.Objects;

class Messages {

    static class Request {

        final long value;

        Request(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Request{" +
                    "value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return value == request.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }


    static class Prepare {

        final long proposalNo;

        Prepare(long proposalNo) {
            this.proposalNo = proposalNo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Prepare)) return false;
            Prepare prepare = (Prepare) o;
            return proposalNo == prepare.proposalNo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(proposalNo);
        }
    }


    static class Promise {

        final long proposalNo;
        final long previousProposalNo;
        final long previousValue;

        Promise(long proposalNo, long previousProposalNo, long previousValue) {
            this.proposalNo = proposalNo;
            this.previousProposalNo = previousProposalNo;
            this.previousValue = previousValue;
        }

        @Override
        public String toString() {
            return "Promise{" +
                    "proposalNo=" + proposalNo +
                    ", previousProposalNo=" + previousProposalNo +
                    ", previousValue=" + previousValue +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Promise promise = (Promise) o;
            return proposalNo == promise.proposalNo &&
                    previousProposalNo == promise.previousProposalNo &&
                    previousValue == promise.previousValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(proposalNo, previousProposalNo, previousValue);
        }
    }

    static class Accept {

        final long proposalNo;
        final long value;
        final ActorRef client;

        Accept(long proposalNo, long value, ActorRef client) {
            this.proposalNo = proposalNo;
            this.value = value;
            this.client = client;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Accept accept = (Accept) o;
            return proposalNo == accept.proposalNo &&
                    value == accept.value &&
                    Objects.equals(client, accept.client);
        }

        @Override
        public int hashCode() {
            return Objects.hash(proposalNo, value, client);
        }
    }


    static class Accepted extends Accept {

        Accepted(long proposalNo, long value, ActorRef client) {
            super(proposalNo, value, client);
        }
    }


    static class Response {

        final long proposalNo;
        final long learnedValue;

        Response(long proposalNo, long learnedValue) {
            this.proposalNo = proposalNo;
            this.learnedValue = learnedValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return proposalNo == response.proposalNo &&
                    learnedValue == response.learnedValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(proposalNo, learnedValue);
        }
    }
}
