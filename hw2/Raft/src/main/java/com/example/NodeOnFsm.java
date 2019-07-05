package com.example;
//import akka.util.FiniteDuration;
//import java.util.concurrent.TimeUnit;

//Duration duration = new FiniteDuration(100, MILLISECONDS);
import akka.actor.ActorRef;
import akka.actor.Props;

import java.time.Duration;
//import scala.concurrent.duration.Duration;

import java.util.*;
import  static com.example.Messages.*;

import akka.actor.*;
//import akka.actor.Props;
//import jdocs.AbstractJavaTest;
import java.util.ArrayList;

import akka.actor.AbstractFSM;

import akka.event.Logging;
import akka.event.LoggingAdapter;


import java.util.concurrent.ThreadLocalRandom;




// value of the total number of nodes is decided in the beginning

/*
    The class is a single entry goes in the entry log of a node
    The class object can also be put in  a message sent between different nodes.
 */
public class NodeOnFsm {


    // states
    public static enum Roles {
        Candidate,
        Follower,
        Leader
    }


    public static final class NodeData {
        /*
        @para nextInd: it is the index that it's about to commit?


         * @para numMember: The total number of members in the cluster of nodes which this node resides in
         * @para totalNumberMember: the total number of actors
         * @para identity: whether this node is leader, follower or candidate
         */


        public ActorRef ownerActorRef;

        int totalNumberMember;
        int voteCount; //the number of votes for become a leader a node received is initially zero
        int currentTerm; // latest term server has seen (initialized to 0 on first boot, increases monotonically)
        Roles currentRole;
        int heartBeatTimeOut;
        public ArrayList<Entry> log;  //log entries; each entry contains command for state machine, and term when entry was received by leader
        boolean ifVoted;  // if this server has voted in current term
        int lastApplied; //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
        int commitIndex; //index of highest log entry known to be committed (initialized to 0, increasesmonotonically)
        LoggingAdapter logHandle;
        // relevant only for leaders
        //for each server, index of the next log entry to send to that server (initialized to leader
        //last log index + 1)
        Map<ActorRef, Integer> nextIndex = new HashMap<ActorRef, Integer>();
        ;// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)


        //relevant only for leaders
        //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        Map<ActorRef, Integer> matchIndex = new HashMap<ActorRef, Integer>();

        public void updateRef(SomeMessage mes) {

            if (nextIndex.get(mes.messageSender) != null) {
                nextIndex.put(mes.messageSender, lastApplied + 1);
            }
            if (matchIndex.get(mes.messageSender) != null) {
                matchIndex.put(mes.messageSender, 0);
            }

        }

        public void sendRef(ActorSelection select) {
            InitializeMessage ames = new InitializeMessage(this);
            select.tell(ames, ames.messageSender);
        }


        public NodeData(ActorRef dataOwner, LoggingAdapter logHandle) {
            // https://doc.akka.io/docs/akka/current/actors.html#upgrade
            // timeOut = ThreadLocalRandom.current().nextInt(150, 300);
            ownerActorRef = dataOwner;
            totalNumberMember = 1;
            voteCount = 0; //the number of votes for become a leader a node received is initially zero
            logHandle = logHandle;
            currentTerm = 0;
            commitIndex = 0;

            currentRole = Roles.Follower;

            //int heartBeatTimeOut = ThreadLocalRandom.current().nextInt(150, 300);
            log = new ArrayList<Entry>();
            log.add(new Entry("hello",0,0));
            ifVoted = false;

        }

        private void allServerRule() {

            if (commitIndex > lastApplied) {

                lastApplied = lastApplied + 1;
                applyToStateMachine();
            }

        }





        public int getTotalNumberMember() {
            return this.totalNumberMember;
        }


        // do it when the node commit new entry
        public void incrementNextInd(ActorRef a) {
            this.nextIndex.put(a, this.nextIndex.get(a) + 1);  // the next index will be the previous one plus one
        }

        public void incrementMatchIndex(ActorRef a) {
            this.matchIndex.put(a, this.matchIndex.get(a) + 1);  // the next index will be the previous one plus one
        }

        // update the commitIndex
        public void checkNforCommitIndex() {

            int Nstart = commitIndex + 1;
            boolean startTodecrease = false;
            int prevCounter = 0;
            for (int N = Nstart; N < Nstart * 10; ++N) {
                if (!startTodecrease) {


                    int counter = 0;
                    for (int i : matchIndex.values()) {
                        if (N <= i) {
                            ++counter;
                        }
                    }
                    if (prevCounter > counter) {
                        startTodecrease = true;
                    }
                    if (counter >= totalNumberMember / 2 && log.get(N).getTerm() == currentTerm) {
                        commitIndex = N;
                        allServerRule(); // check this when commitIndex changed
                        break;
                    }
                    prevCounter = counter;

                }
            }
        }


        public boolean validateMatching(AppendEntryRPC mes) {
            boolean compareResult;
            // as other function are handling whether the mes  and data object have the same term
            if (mes.prevLogTerm == this.log.get(mes.prevLogIndex).getTerm()) {
                compareResult = true;
            } else {
                compareResult = false;
            }
            return compareResult;
        }


        public void handleValidateMatchingResult(boolean validateMatchingResult, AppendEntryRPC mes) {

            if (validateMatchingResult) { // if the preceding term matches
                log.addAll(mes.prevLogIndex + 1, mes.entries);
                AknowledgementMessage akMes = new AknowledgementMessage(this, log.size(), log.size() - 1, true);
                mes.messageSender.tell(akMes, this.ownerActorRef);  // send back the acknow message

            } else {
                AknowledgementMessage akMes = new AknowledgementMessage(this, 0, 0, false); // the matchindex and nextindex doesn't matter in the event of non-match
                log.subList(mes.prevLogIndex, log.size()).clear();
                mes.messageSender.tell(akMes, this.ownerActorRef); // send back the acknow message
            }


        }

        public void checkLeaderCommitVertCommitIndex(AppendEntryRPC mes) {
            if (mes.leaderCommit > this.commitIndex) {
                commitIndex = Math.min(mes.leaderCommit, log.size());
            }

        }


        public boolean handleAppendEntryRPC(AppendEntryRPC mes) {
            boolean result = validateMatching(mes);
            handleValidateMatchingResult(result, mes);
            checkLeaderCommitVertCommitIndex(mes);
            // no matter append succeed or fail, make a new acknowledgement message and send it back
            return result;
        }


        private void applyToStateMachine() {
            // apply log[lastApplied] to state machine
            logHandle.info("apply log to statemachine %d" + lastApplied);


        }

        // @para: Entry: usually an Entry object contained in a message
        public Situation compare(int term) {
            return log.get(log.size()-1).compareTo1(term);
        }

        public ArrayList<Entry> getlog() {
            return this.log;
        }

        public void setVoteCount(int voteN) {
            this.voteCount = voteN;
        }

        public int getVoteCount() {
            return this.voteCount;
        }


        public void newElection() {
            //send out requestVoteRPC to all fellow actors
        }


        //If command received from client: append entry to local log,
        public void receiveFromClient(Entry a) {
            this.getlog().add(a);
        }

        public void checkLastLogIndexVersusNextIndexAndSendLogs() {
            // create array of actors
            for (ActorRef aRef : nextIndex.keySet()) {
                if (getlog().size() >= nextIndex.get(aRef)) {

                    AppendEntryRPC message = new AppendEntryRPC(this, aRef);
                    aRef.tell(message, ownerActorRef);
                }

            }
        }

        public void receiveCheckLastLogIndexVersusNextIndexAndSendLogsResult(AknowledgementMessage mes) {
            if (mes.result == true) {   // this means the append log is successful

                nextIndex.put(mes.messageSender, mes.nextIndex);
                matchIndex.put(mes.messageSender, mes.matchIndex);
            } else {
                this.nextIndex.put(mes.messageSender, this.nextIndex.get(mes.messageSender) - 1);
                //try to send again
                AppendEntryRPC message = new AppendEntryRPC(this, mes.messageSender);
                mes.messageSender.tell(message, ownerActorRef);
            }
        }


        public void incrementTerm() {
            // set a new timer
            currentTerm = currentTerm + 1;

        }

        public void resetVote() {
            this.voteCount = 0;
        }

        public void setIfVoted(boolean a) {
            this.ifVoted = a;
        }

        public boolean getIfVoted() {
            return this.ifVoted;
        }


        static Props props() {
            return Props.create(Node.class, Node::new);
        }


    }


    public static class Node extends AbstractFSM<Roles, NodeData> {
        public static Cancellable cancellable;
        public Roles theRole;

        private  FSM.State<Roles, NodeData>  init(NodeData a) {
            //ElectionTimeout atimeout =
                 //   Class.class.cast( new ElectionTimeout(2));
            System.out.println("line 317");
            ElectionTimeout  atime = new ElectionTimeout(a);
           Cancellable cancellable = getContext().getSystem().
                    scheduler()
                    .schedule(
                            Duration.ZERO, Duration.ofMillis(50),getSelf(), Class.class.cast(atime), getContext().getSystem().dispatcher(), null);

           System.out.println("line323");
           // setTimer("electionTimeout",  Class.class.cast( new RequestVoteRPC(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
            return stay().using(a);
        }

        // this happens when a follower receives request to vote for a specific candidate node
        private FSM.State<Roles, NodeData> followerReceiveRequestVoteRPC(NodeData a, SomeMessage b) {
            receiveLog(b);
            Situation sit = checkToUpdateTerm(a, b); // check if the request comes from the correct term
System.out.println("line 310");
            if (sit == Situation.same) {
                if (a.getIfVoted()) {
                    // if it has already voted then do nothing
                } else { // if it has not yet voted

                    if ((b.term > a.currentTerm) || // the sender asking for note has more current term
                            (b.term == a.currentTerm && (b.logIndex >=a.getlog().size()
                            ))) {
                        System.out.println("line318");
                        VoteMessage aMes = new VoteMessage(a);
                        b.messageSender.tell(aMes, getSelf()); //  send message back to vote for the original message sender to vote for it
                        a.setIfVoted(true); // set the voted to be true so that it does not vote again in this term
                        //final Duration fivesec = Duration.create(5, "millis");

                        //setStateTimeout(state, duration)
                        cancelTimer("electionTimeout");
                        setTimer("electionTimeout", Class.class.cast( new ElectionTimeout(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
                    } else { // else  it does not vote


                    }
                }
                return stay().using(a); // recieve a RequestVoteRPC do not require ti to change status

            } else if (sit == Situation.exceed) { // if it receive message that is more updated than itself, it stays as follower

                return handleOutdatedNode(a,b);

            } else{
                // the term has already being updated
                //
                return stay().using(a);

            }
        }


        private FSM.State<Roles, NodeData> followerReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {
            receiveLog(b);
            Situation situ = checkToUpdateTerm(a, b);
            if (situ == Situation.same) {
                boolean compareAtoB = a.handleAppendEntryRPC(b);  //

                // reset electionTimeout since it receives AppendEntryRPC
                cancelTimer("electionTimeout");
                setTimer("electionTimeout", Class.class.cast( new ElectionTimeout(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if the follower is outdated compared to the leader

                return handleOutdatedNode(a, b);

            } else{ // if the leader is outdated
                // send message back to prompt it to change
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> followerReceiveHeartBeatRPC(NodeData a, SomeMessage b) {
            receiveLog(b);

            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                // reset timer
                cancelTimer("electionTimeout");
                setTimer("electionTimeout", Class.class.cast( new ElectionTimeout(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);

                return stay().using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if the follower  is outdated compared the message sender(leader)
                // update its term--> already done
                return handleOutdatedNode(a, b);

            } else { // if the  follower is more updated than the leader
                // send a message back --> already done
                return stay().using(a); // do not need to reset timeout
            }
        }

        // update the term and reset values
        // ignore followerReceiveVoteMessage
        private FSM.State<Roles, NodeData> followerTimeOut(NodeData a, ElectionTimeout b) {
            System.out.println("line388");

            a.incrementTerm();   // increment current term before sending messages
            RequestVoteRPC message = new RequestVoteRPC(a);

            ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
           System.out.println(selection);
            selection.tell(message, getSelf()); // send the RequestResource message to all the selection and specify the sender is this sender

            a.setIfVoted(true);// set just in case
            a.setVoteCount(1); // set just in case , it becomes candidate therefore vote for itself first
            a.currentRole = Roles.Candidate;
            theRole = Roles.Candidate;
            return goTo(Roles.Candidate).using(a);  // stay but reset the timeout
        }

        // receiver: Candidate   only candidate can receive Vote Message
        //              candidate: step down if discover  higher term
        //              leader: step down if currentTerm changes
        //
        private FSM.State<Roles, NodeData> candidateReceiveVoteMessage(NodeData a, VoteMessage b) {
            receiveLog(b);

            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {

                if (a.getVoteCount() > a.getTotalNumberMember() / 2) { //if majority votes then it becomes the leader for the current term
                    // send out heartbeat message
                    HeartBeatRPC heartB = new HeartBeatRPC(a); // make the first heart beat message
                    ActorSelection selection = getContext().actorSelection("/user/*");
                    selection.tell(heartB, getSelf()); // send heartbeat to everyone else
                    // This will schedule to send the Tick-message
                    // to the tickActor after 0ms repeating every 50ms
                    cancellable = context().system().scheduler()
                            .schedule(Duration.ofMillis(0), Duration.ofMillis(10),
                                    new Runnable() {
                                        @Override
                                        public void run() {
                                            getContext().actorSelection("/user/*").tell(new HeartBeatRPC(a), getSelf());  // send heartbeat
                                            a.checkLastLogIndexVersusNextIndexAndSendLogs();    // check and see if to send AppendEntryRPC
                                        }
                                    },
                                    context().system().dispatcher());

                    a.currentRole = Roles.Leader;
                    return goTo(Roles.Leader);

                } else {   // if the situation is the same
                    return stay().using(a);
                }
            } else if (situ == Situation.exceed) {  // if the candidate  is outdated compared the message sender(follower)

                return handleOutdatedNode(a,b);
                // stay but reset the timeout

            } else { // if the  candidate is more updated than the message sender

                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> candidateReceiveHeartBeatRPC(NodeData a, SomeMessage b) {
            receiveLog(b);

            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                // reset timer
                a.currentRole = Roles.Leader;
                return goTo(Roles.Leader).using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if the follower  is outdated compared the message sender(leader)
                // update its term
                return handleOutdatedNode(a,b);  // stay but reset the timeout

            } else { // if the  follower is updated
                //
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> candidateReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {
            receiveLog(b);

            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                // as a way to prolong the current timeout
                a.currentRole = Roles.Leader;
                return goTo(Roles.Leader); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // update its term
                return stay().using(a).forMax(Duration.ofMillis(5));  // stay but reset the timeout

            } else { // if updated

                return stay().using(a); // do not need to reset timeout
            }
        }





        // this happends when it did not receive heartbeatRPC or receiveFromClientRPC and timeOUT, therefore there's no received message as parameter
        private FSM.State<Roles, NodeData> candidateTimeOut(NodeData a, ElectionTimeout elec) {
            // update its term number
            //
            System.out.println("line 494");
            a.incrementTerm();   //  it will increment now that it changes its state
            a.resetVote(); // just in case, reset the vote count to zero
            a.setVoteCount(1); // vote for itself
            a.setIfVoted(true); // it can't vote anymore in this term
            RequestVoteRPC aCurMes = new RequestVoteRPC(a);  // Message that ask any
            ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
            selection.tell(aCurMes, getSelf());
            cancelTimer("electionTimeout");

            setTimer("electionTimeout",Class.class.cast(new ElectionTimeout(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);

            //stay().using(a).timeout(Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)));
            return stay().using(a);

        }


        private FSM.State<Roles, NodeData> leaderReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {

                // do nothing , not going to happen
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if  is outdated compared the message sender(follower)
                // reset its values and step down

                return handleOutdatedNode(a, b);
                // stay but reset the timeout

            } else { // if is more updated than the message sender
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> candidateReceiveRequestVoteRPC(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);

            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                return stay().using(a); // do nothing
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // update its term
                return handleOutdatedNode(a, b);  // stay but reset the timeout

            } else { // if updated

                return stay().using(a); // do not need to reset timeout
            }
        }

        // candidateReceiveRequestVoteRPC--> ignore since only


        //when leader to follower can only happens when it get message with higher term number
        // the term number update will be handled by checkToUpdateTerm


        private FSM.State<Roles, NodeData> leaderReceiveVoteMessage(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {

                // do nothing , not going to happen
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if  is outdated compared the message sender(follower)
                // reset its values and step down

                return handleOutdatedNode(a, b);
                // stay but reset the timeout

            } else { // if is more updated than the message sender
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> leaderReceiveClientMessage(NodeData a, RawMessage b) {
            receiveLog(b);

            a.log.add(new Entry(b.message, a.currentTerm,a.log.size()));
            return stay().using(a);
        }


        private FSM.State<Roles, NodeData> handleOutdatedNode(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.currentTerm = b.term;
            a.resetVote();
            a.setIfVoted(false);
            a.currentRole = Roles.Follower;
            return goTo(Roles.Follower).using(a);
        }

        private FSM.State<Roles, NodeData> leaderReceiveHeartBeatRPC(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                // reset timer
                // ignore, we are already leader.
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // reset data update its term

                return handleOutdatedNode(a, b);  // stay but reset the timeout
            } else { // if the  follower is updated
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> leaderReceivedAknowledgementRPC(NodeData a, AknowledgementMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                // as a way to prolong the current timeout

                a.receiveCheckLastLogIndexVersusNextIndexAndSendLogsResult(b);  // receive aknowledgement message
                a.checkNforCommitIndex(); // update the commit index;

                return stay().using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if situ is outdated compared to the message sender(leader)
                // update its term
                return handleOutdatedNode(a, b); // stay but reset the timeout
            } else { // if updated
                return stay().using(a); // ignore
            }
        }


        private FSM.State<Roles, NodeData> leaderReceiveRequestVoteRPC(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                return stay().using(a); // do nothing since it's already a leader
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // update its term
                //reset
                return handleOutdatedNode(a, b);// stay but reset the timeout

            } else { // if updated

                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> leaderReceiveFailureMessage(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = checkToUpdateTerm(a, b); // received a Situation message
            if (situ == Situation.same) {
                return stay().using(a); // do nothing since it's already a leader
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                return handleOutdatedNode(a, b);// stay but reset the timeout

            } else { // if updated
                return stay().using(a); // do not need to reset timeout
            }
        }


        // no matter the receiver is candidate, leader, or follower, it always result its data state and change/stay to a follower
        public Situation checkToUpdateTerm(NodeData a, SomeMessage b) {
            receiveLog(b);
            a.updateRef(b);
            Situation situ = a.compare(b.term);
            if (situ == Situation.same) {
                // do nothing
            } else if (situ == Situation.exceed) {
                // update the term
                // a becomes follower therefore reselt data members
                a.currentTerm = b.term;
                a.resetVote();
                a.setIfVoted(false);

            } else { // if the original message sender is outdated.

                // do nothing , as the original message sender will be notified it's outdated eventually
            }
            return situ;
        }
        final LoggingAdapter logHandle = Logging.getLogger(getContext().getSystem(), this);


        //startWith(Candidate, Uninitialized);  // start with candidate state and empty data
        {
            NodeData a= new NodeData(getSelf(), logHandle);
            startWith(Roles.Candidate,a); // initialize NodeData associated with the Node
            init(a);
               System.out.println("line 684");
// it starts to wait for the response of the other grab


            when(Roles.Candidate,
                    matchEventEquals(ElectionTimeout.class,NodeData.class,
                            (theMessage, data) -> candidateTimeOut(data, ElectionTimeout.class.cast(theMessage))).
                            event(
                            AppendEntryRPC.class, NodeData.class,
                                    (theMessage, data) -> candidateReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> candidateReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> candidateReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))).
                            event(VoteMessage.class,
                                    (theMessage, data) -> candidateReceiveVoteMessage(data, VoteMessage.class.cast(theMessage)))

            );
            System.out.println("line 716");

            when(Roles.Follower,
                    matchEventEquals(ElectionTimeout.class,
                            (theMessage, data) -> {
                        System.out.println("line 721");
                        return followerTimeOut(data, ElectionTimeout.class.cast(theMessage));}
                    ).
                            event(AppendEntryRPC.class,
                                    (theMessage, data) -> followerReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> followerReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> followerReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))));


            when(Roles.Leader,
                    matchEvent(AppendEntryRPC.class,
                            (theMessage, data) -> leaderReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> leaderReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> leaderReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))).
                            event(VoteMessage.class,
                                    (theMessage, data) -> leaderReceiveVoteMessage(data, VoteMessage.class.cast(theMessage))).
                            event(RawMessage.class,
                                    (theMessage, data) -> leaderReceiveClientMessage(data, RawMessage.class.cast(theMessage))).
                            event(AknowledgementMessage.class,
                                    (theMessage, data) -> leaderReceivedAknowledgementRPC(data, AknowledgementMessage.class.cast(theMessage)))

            );




            onTransition(
                    matchState(Roles.Leader, Roles.Follower,
                            () -> {
                                System.out.println("line 750");
                                cancelTimer("electionTimeout");
                                cancellable.cancel();  // cancel the timer
                            }). // set follower timeout
                            state(Roles.Candidate, Roles.Follower,
                            () -> {
                                System.out.println("line 756");
                                cancelTimer("electionTimeout"); // reset the timer

                                setTimer("electionTimeout",Class.class.cast( new ElectionTimeout(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), true);
                            }).
                            state(Roles.Candidate, Roles.Leader, () ->{
                                System.out.println("line 761");
                                    cancelTimer("electionTimeout");}).  // leader don't have timer
                            state(Roles.Follower, Roles.Candidate,
                            (f, t) -> {
                                System.out.println("line 764");
                                cancelTimer("electionTimeout"); // reset timer

                                setTimer("electionTimeout",  new ElectionTimeout(a), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), true);

                            }).  // set leader time out
                            state(Roles.Leader, Roles.Candidate,
                            (l, f) -> { // this won't happen
                            }

                    ).
                            state(Roles.Follower, Roles.Leader,
                                    (l, f) -> {   // this won't happen either
                                    }));


            initialize();


        }


        

        public void receiveLog(AbstractMessage m) {
                logHandle.info("{} received  {} ", getSelf(),m.toString());

        }
/*
        public void sendLog(SomeMessage m) {
            if (m instanceof RequestResource) {
                this.log.info("{} sent *** Request Resource message: {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            } else if (m instanceof ReleaseResource) {
                this.log.info("{} sent *** Release Resource messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            } else if (m instanceof Aknowledgement) {
                this.log.info("{} sent *** Aknowledgement messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            }
        }

        */
    }


}