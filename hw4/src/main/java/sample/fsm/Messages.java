package sample.fsm;

import akka.actor.ActorRef;
import scala.Some;

import java.sql.Time;
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

    public static final class HeartBeatRPC extends  SomeMessage {
        public HeartBeatRPC(NodeOnFsm.NodeData a) {
            super(a); // it does not matter which actor this heartBeatRPC got sent to
        }

    }

    public static final class UpdateBackendNode extends SomeMessage{
        public ActorRef backendRef;
        public UpdateBackendNode(ActorRef aRef){
            backendRef = aRef;
        }

    }

    public static final class  AcknowledgeWriteProcedure{
        int ticketNumReserved;
        int ticketReservedByMotivator;
        public AcknowledgeWriteProcedure(int num, int num1){
            ticketNumReserved = num;
            ticketReservedByMotivator = num1;
        }

    }

    public static final class FailureMessage{
        public FailureMessage(){

        }
    }
    // make a CopyOfStateMachine message  when receive a client requesting a copy of the statemachine .
    public static final class CopyOfStateMachine extends  SomeMessage {
        int ticketNum;
        int ticketNumCommitted;

        public CopyOfStateMachine(NodeOnFsm.NodeData a) {
            super(a); // it does not matter which actor this heartBeatRPC got sent to
            ticketNumCommitted = a.aMachine.tickeNumCommitted;
            ticketNum = a.aMachine.ticketNum;   // get the ticket number from the statemachine
        }

    }





    public static  class AppendEntryRPC extends SomeMessage {
        /*
         @ para a is the node data that is sending the message
         @ para theEntry is the entry that
         */
        ActorRef leaderId;
        public int prevLogTerm;
        public int prevLogIndex;
        public int leaderCommit;
        public ArrayList<Entry> entries;


        public AppendEntryRPC(NodeOnFsm.NodeData a, ActorRef receiverRef) {
            super(a);
            leaderId = super.messageSender;
            System.out.println(String.format("the leader actorRef is %s ", receiverRef));
            leaderCommit = a.commitIndex;
            if (a.serverLog.isEmpty()) {
                System.out.println("In this AppendEntryRPC and currently the leader serverlog is empty ");
                entries = a.serverLog;
                prevLogTerm = 0;
                prevLogIndex = 0;

            } else {
                System.out.println("In this AppendEntryRPC and currently the leader serverlog is not empty ");
                for (int i = 0; i < a.serverLog.size(); ++i) {
                  //  System.out.println(a.serverLog.get(i).logMessage);
                }
                System.out.println(String.format("Is getting serverLog from %d to %d ", a.nextIndex.get(receiverRef) , a.serverLog.size()) );
                entries = new ArrayList<>(a.serverLog.subList(a.nextIndex.get(receiverRef), a.serverLog.size()));

                if (a.nextIndex.get(receiverRef) == 0) {
                    prevLogTerm =0;
                    prevLogIndex =0;
                }else{
                    prevLogTerm = a.serverLog.get(a.nextIndex.get(receiverRef) - 1).getTermIndex(); // get the term index of precedding
                    prevLogIndex = a.serverLog.get(a.nextIndex.get(receiverRef) - 1).getLogIndex(); // get the term index of precedding
                }
            }
        }

        public String toString(){
            return  "AppendEntryRPC" + super.toString() + String.format(" leaderCommit: %d prevLogTerm: %d prevLogIndex: %d  attachedLogSize: %d",
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
            return String.format(" sender %s term: %d  ", messageSender, term);
        }
        // Any message receival would compare its term number with the receiver node first


    }


    public static class RequestVoteRPC extends SomeMessage {
       public int term;
       public ActorRef candidateId; //candidate requesting vote
       public int lastLogIndex; //index of candidate’s last log entry (
       public int lastLogTerm; //term of candidate’s last log entry

        public RequestVoteRPC(NodeOnFsm.NodeData a){
            super(a);
            candidateId = super.messageSender;
            term = a.currentTerm;
            if(a.serverLog.size()==0){
                lastLogIndex=0;
                lastLogTerm=0;
            }else {
                lastLogIndex = a.serverLog.get(a.serverLog.size() - 1).getLogIndex();
                lastLogTerm = a.serverLog.get(a.serverLog.size() - 1).getTermIndex();
            }}

        @Override
        public String toString() {
            return "RequestVoteRPC"+ super.toString() +String.format(" lastLogIndex: %d  lastLogTerm %d", lastLogIndex, lastLogTerm) ;
        }
    }


   public static class TermMessage extends SomeMessage{
        public int term;
        public int logLength;
        public TermMessage( NodeOnFsm.NodeData a){
            term = a.currentTerm;
            logLength = a.serverLog.size();
        }
   }


    public static class AcknowledgementMessage extends SomeMessage {
        int nextIndex;
        int matchIndex;
        boolean result;
        public AcknowledgementMessage(  NodeOnFsm.NodeData a, int nex, int mat,boolean res) {
            super(a);
            nextIndex =  mat;
            matchIndex = nex;
            result = res;
        }

        @Override
        public String toString() {
            return "AcknowledgementMessage   " +String.format("nextIndex: %d matchIndex: %d  result: %s", nextIndex, matchIndex, result);
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