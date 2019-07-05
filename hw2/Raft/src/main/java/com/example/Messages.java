package com.example;

import akka.actor.ActorRef;

import java.util.ArrayList;

public class Messages {
    public static final class ElectionTimeout extends SomeMessage {
        public int random;
        public ElectionTimeout(NodeOnFsm.NodeData a) {
            random = a.currentTerm;
        }
    }


    public static final class InitializeMessage extends  SomeMessage{

        public InitializeMessage(NodeOnFsm.NodeData a){
            super(a);
        }

    }

    public static final class HeartBeatRPC extends AppendEntryRPC {
        public HeartBeatRPC(NodeOnFsm.NodeData a) {
            super(a,null); // it does not matter which actor this heartBeatRPC got sent to
        }

    }

    public static  class AppendEntryRPC extends SomeMessage {
        /*
         @ para a is the node data that is sending the message
         @ para theEntry is the entry that
         */
        ActorRef leaderId = super.messageSender;
        public int prevLogTerm;
        public int prevLogIndex;
        public int leaderCommit;
        public ArrayList<Entry> entries;


        public AppendEntryRPC(NodeOnFsm.NodeData a, ActorRef receiverRef) {
            super(a);
            leaderId = super.messageSender;
            leaderCommit = a.commitIndex;
            entries = (ArrayList<Entry>) a.log.subList(a.nextIndex.get(receiverRef), a.log.size());
            prevLogTerm = a.log.get(a.nextIndex.get(receiverRef) - 1).getTermIndex(); // get the term index of precedding
            prevLogIndex = a.log.get(a.nextIndex.get(receiverRef) - 1).getLogIndex(); // get the term index of precedding


        }
        public String toString(){
            return  "AppendEntryRPC" + super.toString() + String.format(" leaderCommit: {} prevLogTerm:{} prevLogIndex:{}  attachedLogSize: {}",
                    leaderCommit, prevLogTerm, prevLogIndex, entries.size() );
        }


    }
    // class of message sent between actors of candidate, follower, leader.
    public static abstract class SomeMessage implements AbstractMessage {
        /*
        @para from: the sender ref who sends the message
        @para theMessage: the message which sent by the sender actor
        @para term: of last term of the in the log of the sender
        @para logIndex: the last index in the log of the sender
        @para Entry the log Entry that this message sends along
         */
        ActorRef messageSender;
        protected int term;  //leader’s term
        protected int logIndex;

        public SomeMessage( ) {
            this.messageSender = null;
            this.term =-1;
        }

        public SomeMessage( NodeOnFsm.NodeData a) {
            this.messageSender = a.ownerActorRef;
            this.term = a.currentTerm;
        }

        @Override
        public String toString() {
            return String.format(" sender {} term: {} logIndex: {} ", messageSender, term, logIndex);
        }
        // Any message receival would compare its term number with the receiver node first


    }


    public static class RequestVoteRPC extends SomeMessage {

        ActorRef candidateId; //candidate requesting vote
        int lastLogIndex; //index of candidate’s last log entry (
        int lastLogTerm; //term of candidate’s last log entry

        public RequestVoteRPC(NodeOnFsm.NodeData a){
            super(a);
            candidateId = super.messageSender;
            lastLogIndex = a.log.get(a.log.size()-1).getLogIndex();
            lastLogTerm = a.log.get(a.log.size()-1).getTermIndex();
        }

        @Override
        public String toString() {
            return "RequestVoteRPC"+ super.toString() +String.format(" lastLogIndex: {}  lastLogTerm {}", lastLogIndex, lastLogTerm) ;
        }
    }


    public static class RawMessage implements AbstractMessage{
        public String message;
        public RawMessage(String mess){
            message =mess;
        }

        @Override
        public String toString() {
            return  message;
        }
    }




    public static class AknowledgementMessage extends SomeMessage {
        int nextIndex;
        int matchIndex;
        boolean result;
        public AknowledgementMessage(  NodeOnFsm.NodeData a, int nex, int mat,boolean res) {
            super(a);
            nextIndex =  mat;
            matchIndex = nex;
            result = res;
        }

        @Override
        public String toString() {
            return "AknowledgementMessage   "+super.toString() +String.format("nextIndex: {}  matchIndex: {}  result: {}", nextIndex, matchIndex, result);
        }
    }


    public static class VoteMessage extends SomeMessage {
        public VoteMessage(NodeOnFsm.NodeData a){
            super(a);
        }

        @Override
        public String toString() {
            return "VoteMessage  "+super.toString();
        }
    }



}