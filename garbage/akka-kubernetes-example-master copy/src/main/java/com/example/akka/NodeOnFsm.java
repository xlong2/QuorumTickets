package com.example.akka;
//import akka.util.FiniteDuration;
//import java.util.concurrent.TimeUnit;
//Duration duration = new FiniteDuration(100, MILLISECONDS);

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import static com.example.akka.Messages.*;

//import scala.concurrent.duration.Duration;
//import akka.actor.Props;
//import jdocs.AbstractJavaTest;

// visit the screen
// value of the total number of nodes is decided in the beginning

/*
    The class is a single entry goes in the entry serverLog of a node
    The class object can also be put in  a message sent between different nodes.
    the class
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
        public ActorRef leaderRef;
        public int totalNumberMember;
        public int voteCount; //the number of votes for become a leader a node received is initially zero
        public int currentTerm; // latest term server has seen (initialized to 0 on first boot, increases monotonically)
        public Roles currentRole;
        int heartBeatTimeOut;
        public ArrayList<Entry> serverLog;  //serverLog entries; each entry contains command for state machine, and term when entry was received by leader
        boolean ifVoted;  // if this server has voted in current term
        int lastApplied; //index of highest serverLog entry applied to state machine (initialized to 0, increases monotonically)
        int commitIndex; //index of highest serverLog entry known to be committed (initialized to 0, increasesmonotonically)
        LoggingAdapter logHandle;
        public  StateMachine aMachine;
        public Logger logger ;
        public FileHandler fh;

        public class StateMachine {
            int ticketNum;
            int tickeNumCommitted;


            public StateMachine(int totalNum) {
                ticketNum = totalNum;
                tickeNumCommitted = totalNum;
            } // initiate the stateMachine that should exist in each node.

            public void increment() {
                ++ticketNum;
            }// increment the ticket number

           // public void decrement() {
            //    --ticketNum;
            //} // decrement the ticket number






            // write the current state of the state Machine into a message
            public CopyOfStateMachine writeMachine(NodeData a, Client.UnstableReadRequest requestMes){
                CopyOfStateMachine aMes= new CopyOfStateMachine( a ); // The aMes contains the number of ticket requested and the number of ticket remained.

                return aMes; // the number of ticket to be sent back
            }

            public void applyLogRegardless(Entry log){ //
                // execute all entry regardless whether they are committed or not
                System.out.println(String.format("inside applyLogRegardless and the ticketNumTaken is %d ", log.ticketNumTaken) );
                ticketNum = ticketNum -log.ticketNumTaken;
                  //  decrement();  // here any apply is the same as decrement
            }

            public void applyLog(Entry log, int entryIndex){
                if(entryIndex<= commitIndex){  // if entryIndex is smaller or the same as the commitIndex
                    //run the log
                  //  decrement();
                    tickeNumCommitted = tickeNumCommitted -log.ticketNumTaken;
                }

            }



        }
        // relevant only for leaders
        //for each server, index of the next serverLog entry to send to that server (initialized to leader
        //last serverLog index + 1)
        Map<ActorRef, Integer> nextIndex = new HashMap<ActorRef, Integer>();
        ;// for each server, index of the next serverLog entry to send to that server (initialized to leader last serverLog index + 1)


        //relevant only for leaders
        //for each server, index of highest serverLog entry known to be replicated on server (initialized to 0, increases monotonically)
        Map<ActorRef, Integer> matchIndex = new HashMap<ActorRef, Integer>();

        public void reinitializeNextIndex(SomeMessage mes) {

            if (nextIndex.get(mes.messageSender) == null) { // if it has never
                //if(serverLog.size())
                int leaderLastIndex = serverLog.size();  // the serverlog size can be zero
                nextIndex.put(mes.messageSender,leaderLastIndex) ;
                System.out.println(String.format("Intilized NextIndexM %s  's nextIndex to %d", mes.messageSender, serverLog.size()));
            }


            if (matchIndex.get(mes.messageSender) == null) {
                matchIndex.put(mes.messageSender, 0);
                System.out.println(String.format("updated the sender %s  's matchIndex to %d", mes.messageSender, 0));
            }

        }



        public void sendRef(ActorSelection select) {
            InitializeMessage ames = new InitializeMessage(this);
            select.tell(ames, ames.messageSender);
        }

        // totalNum : total number of tickets
        public NodeData(ActorRef dataOwner, LoggingAdapter logHandle, int nodeNum, int totalNum) {
            // https://doc.akka.io/docs/akka/current/actors.html#upgrade
            // timeOut = ThreadLocalRandom.current().nextInt(150, 300);

            ownerActorRef = dataOwner;
          //  Logger logger = Logger.getLogger("MyLog");
            voteCount = 0; //the number of votes for become a leader a node received is initially zero
           // logHandle = logHandle;
            currentTerm = 0;
            commitIndex = 0;
            // This block configure the logger with handler and formatter
            //try{
            //fh = new FileHandler(ownerActorRef.toString().split("/")[4] + "_file.txt"
            //);}catch(Exception e){e.printStackTrace();}
            //logger.addHandler(fh);
            //SimpleFormatter formatter = new SimpleFormatter();
            //fh.setFormatter(formatter);
            lastApplied =0;
            totalNumberMember = nodeNum;   // set the total number of nodes
            currentRole = Roles.Follower;
            aMachine = new StateMachine(totalNum); //initiate the totalNum

            //int heartBeatTimeOut = ThreadLocalRandom.current().nextInt(150, 300);
            serverLog = new ArrayList<Entry>();
            ifVoted = false;

        }


        public void displayServerLog() {
            if (serverLog.isEmpty()) {
                System.out.println("log is empty");
            } else {
                for (int i = 0; i < serverLog.size(); ++i) {
                    //System.out.println("line 124");
                    System.out.println(serverLog.get(i).toString()+"| commitIndex "+commitIndex);
                }
            }
        }

        private void allServerRule() {
            System.out.println("in allServerRule");



            /*
            try {



                // the following statement is used to log any messages
               // logger.info("My first log");
                int initialLastApplied = lastApplied;
                int templastApplied = lastApplied;
                while(templastApplied< commitIndex) {   // apply to stateMachine uptil the commit index

                    templastApplied = templastApplied + 1;
                   // ArrayList<Entry> alist = new ArrayList<> (serverLog.subList(lastApplied, lastApplied));
                   logger.info( String.format("%d %s",serverLog.get(templastApplied).ticketNumTaken,  serverLog.get(templastApplied).toString()));
                   // out.writeObject(alist);
                    //applyToStateMachine(lastApplied);  // apply the entry to state machine uptill the commit index

                }

            } catch (SecurityException e) {
                e.printStackTrace();
            }
*/

            try {
                ObjectOutputStream out;
                String fileName = ownerActorRef.toString().split("/")[4] + "_file.txt";
                System.out.println(ownerActorRef.toString());
                File f = new File(fileName);
                if(f.exists() && !f.isDirectory()) {
                    // do something

                    out = new ObjectOutputStream(new FileOutputStream(fileName));
                }else{
                    out = new ObjectOutputStream(new FileOutputStream(fileName));
                }
                System.out.println(String.format("is converting to state machine %s ", fileName));
                int initialLastApplied = lastApplied;
                int templastApplied = lastApplied;
                while(templastApplied< commitIndex) {   // apply to stateMachine uptil the commit index

                    templastApplied = templastApplied + 1;
                        ArrayList<Entry> alist = new ArrayList<> (serverLog.subList(lastApplied, lastApplied));
                        out.writeObject(alist);
                        //applyToStateMachine(lastApplied);  // apply the entry to state machine uptill the commit index

                }

                if(initialLastApplied < templastApplied){
                    ArrayList<Entry> alist = new ArrayList<> (serverLog.subList(0, templastApplied));
                    out.writeObject(alist);
                }



                out.close();
                // logHandle.info("apply serverLog to statemachine %d" + lastApplied);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }


            //System.out.println("at line 231 in allServerRule ");
            while(lastApplied< commitIndex) {   // apply to stateMachine uptil the commit index
                System.out.println(String.format("Comparing lastApplied %d to commitIndex %d", lastApplied, commitIndex));
                lastApplied = lastApplied + 1;
                applyToStateMachine(lastApplied);  // apply the entry to state machine uptill the commit index

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

            for (int N = Nstart; N < serverLog.size(); ++N) {
               // if (!startTodecrease) {


                    int counter = 0;
                    for (int i : matchIndex.values()) {
                        if (N <= i) {
                            ++counter;
                        }
                    }
                    // ever changed
                    if (counter >= totalNumberMember / 2 && serverLog.get(N).getTerm() == currentTerm) {
                        commitIndex = N;
                        allServerRule(); // check this when commitIndex changed
                        //if(startTodecrease) {
                         //   break;
                        //}
                    }
                    //prevCounter = counter;

                //}
            }// end of the for loop

        }


        public boolean validateMatching(AppendEntryRPC mes) {
            boolean compareResult;
            System.out.println(String.format("line 183 this.serverLog.isEmpty() is %b  and mes.prevLogTerm is %d",this.serverLog.isEmpty() , mes.prevLogTerm));
            // as other function are handling whether the mes  and data object have the same term
            System.out.println("line 182 ");
            if (this.serverLog.isEmpty() || mes.prevLogTerm==0 ){
                return true; //proceed to append no matter what;;
            } else if(mes.prevLogIndex>= this.serverLog.size()){
                return false;
            }else if (mes.prevLogTerm == this.serverLog.get(mes.prevLogIndex).getTerm()) {
                //System.out.println("line 189 ");   // also proceed to append if the log term matches
                compareResult = true;
            } else {
                compareResult = false;  // else
            }
            return compareResult;
        }


        public void handleValidateMatchingResult(boolean validateMatchingResult, AppendEntryRPC mes) {
            System.out.println("Inside line 311");
            if (validateMatchingResult) { // if the preceding term matches
                System.out.println("line 194");
                if (serverLog.isEmpty()) {
                    serverLog=mes.entries;
                    System.out.println(String.format("just added some entries !"));
                } else if(mes.prevLogIndex==0) {
                    serverLog = mes.entries;

                }else{
                    serverLog.addAll(mes.prevLogIndex + 1, mes.entries);  // increment the serverLog index
                    System.out.println("line 194 after");
                }
                AcknowledgementMessage akMes = new AcknowledgementMessage(this, serverLog.size(), serverLog.size() - 1, true);
                mes.messageSender.tell(akMes, this.ownerActorRef);  // send back the acknowledge message

            } else {
                System.out.println("Inside 267 line;");
                AcknowledgementMessage akMes = new AcknowledgementMessage(this, 0, 0, false); // the matchindex and nextindex doesn't matter in the event of non-match
               int serverlogEnd = serverLog.size()-1;
                if(mes.prevLogIndex < serverlogEnd) {
                    serverLog.subList(mes.prevLogIndex,serverlogEnd).clear();
                }
                mes.messageSender.tell(akMes, this.ownerActorRef); // send back the acknow message
            }


        }

        public void checkLeaderCommitVertCommitIndex(AppendEntryRPC mes) {
            if (mes.leaderCommit > this.commitIndex) {
                commitIndex = Math.min(mes.leaderCommit, serverLog.size());
            }

        }


        public boolean handleAppendEntryRPC(AppendEntryRPC mes) {
            System.out.println("inside handleAppendEntryRPC");
            boolean result = validateMatching(mes);
            handleValidateMatchingResult(result, mes);
            checkLeaderCommitVertCommitIndex(mes);
            allServerRule(); // the commit index might have changed, so we need to apply entries to the state machine and update the last applied
            // no matter append succeed or fail, make a new acknowledgement message and send it back
            return result;
        }



        // @para: Entry: usually an Entry object contained in a message
        public Situation compare(int term) {

            int term1 = this.currentTerm;
            //System.out.println(String.format("  and the node term is %d and the message term is %d", term1, term));
            if (term > term1) {
                return Situation.exceed;
            } else if (term == term1) {
                return Situation.same;
            } else {
                return Situation.behind;
            }
        }
        public class AppendableObjectOutputStream extends ObjectOutputStream {
            public AppendableObjectOutputStream(OutputStream out) throws IOException {

                super(out);


            }
            @Override
            protected void writeStreamHeader() throws IOException {
                // do not write a header
            }
        }

        private void applyToStateMachine(int index) {
            // apply log[lastApplied] to state machine
            // serialize the serverlog
            System.out.println("inside applyToStateMachine\n ");
            aMachine.applyLogRegardless(serverLog.get(index));
            aMachine.applyLog(serverLog.get(index),commitIndex);
            /*
            try {
                ObjectOutputStream out;
                String fileName = ownerActorRef.toString().split("/")[3] + "_file.txt";
                File f = new File(fileName);
                if(f.exists() && !f.isDirectory()) {
                    // do something

                     out = new ObjectOutputStream(new FileOutputStream(fileName));
                }else{
                     out = new AppendableObjectOutputStream(new FileOutputStream(fileName));
                }
                System.out.println(String.format("is converting to state machine %s ", fileName));

                ArrayList<Entry> alist = new ArrayList<> (serverLog.subList(0, lastApplied));
                out.writeObject(alist);
                out.close();
               // logHandle.info("apply serverLog to statemachine %d" + lastApplied);
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
            */


        }

        public ArrayList<Entry> getlog() {
            return this.serverLog;
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
            this.serverLog.add(a);
        }

        public void checkLastLogIndexVersusNextIndexAndSendLogs(ActorRef a ) {
            // create array of actors
            System.out.println("inside checkLastLogIndexVersusNextIndexAndSendLogs");
            System.out.println("line 274   ");
            System.out.println("the actorRef itself is "+a);
            for (ActorRef aRef : nextIndex.keySet()) {
                System.out.println(String.format("iterating through nextIndex  %s:  %d", aRef, nextIndex.get(aRef)));
                //System.out.println("updating checkLastLogIndex ");
               // System.out.println(String.format("inside checkLastLogIndexVersusNextIndexAndSendLogs  logsize is %d  and nextIndex is %d",serverLog.size() , nextIndex.get(aRef)) );
                System.out.println(String.format("the log size is %d", serverLog.size()));
                if (serverLog.size() >= nextIndex.get(aRef)) {
                    System.out.println(String.format(" the leader actor logsize %d is larger than the nextind %d of follower", serverLog.size(), nextIndex.get(aRef)));

                    AppendEntryRPC message = new AppendEntryRPC(this, aRef);
                    aRef.tell(message, ownerActorRef);
                }

            }
        }


        public void receiveCheckLastLogIndexVersusNextIndexAndSendLogsResult(AcknowledgementMessage mes) {
            if (mes.result == true) {   // this means the append serverLog is successful for the AppendEntry RPC receiver
                //System.out.println("line 286");
                nextIndex.put(mes.messageSender, mes.nextIndex);
                matchIndex.put(mes.messageSender, mes.matchIndex);
            } else {
                //System.out.println("line 290");

                int newNextIndex = this.nextIndex.get(mes.messageSender) - 1;
                if(newNextIndex<0){
                    newNextIndex =0;
                }

                this.nextIndex.put(mes.messageSender, newNextIndex);
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

        public void updateTerm(NodeData a, SomeMessage b) {
            a.currentTerm = b.term;
        }
    }


    public static class Node extends AbstractFSM<Roles, NodeData> {
        public static Cancellable cancellable;
        public Roles theRole;
        Cluster cluster = Cluster.get(getContext().system());

        private FSM.State<Roles, NodeData> init(NodeData a) {
            //ElectionTimeout atimeout =
            //   Class.class.cast( new ElectionTimeout(2));
            setTimer("electionTimeout", "timeOut", Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
           /*
            Cancellable cancellable = getContext().getSystem().
                    scheduler()
                    .schedule(
                            Duration.ZERO, Duration.ofMillis(50),getSelf(), "timeOut", getContext().getSystem().dispatcher(), null);
            */
            // setTimer("electionTimeout",  Class.class.cast( new RequestVoteRPC(a)), Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
            return stay().using(a);
        }


        //subscribe to cluster changes
        @Override
        public void preStart() {
            //#subscribe
            cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                    ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class);
            //#subscribe
        }


        private FSM.State<Roles, NodeData> ifToVote(NodeData a, RequestVoteRPC b) {
            System.out.println(String.format("the b.term %d | a.currentTerm is %d | b.logIndex is %d  | a.serverLog.size() is %d  |a.ifVoted is %b", b.term, a.currentTerm, b.lastLogIndex, a.serverLog.size(), a.ifVoted));
            // System.out.println("line 355");
            if ((b.term > a.currentTerm) || // the sender asking for note has more current term
                    (b.term == a.currentTerm && (b.lastLogIndex > a.serverLog.size()
                    ))) {
                if (a.ifVoted == false) {
                    a.currentTerm = b.term;
                    VoteMessage aMes = new VoteMessage(a);
                    b.messageSender.tell(aMes, getSelf()); //  send message back to vote for the original message sender to vote for it
                    a.setIfVoted(true); // set the voted to be true so that it does not vote again in this term
                    //final Duration fivesec = Duration.create(5, "millis");

                    //setStateTimeout(state, duration)
                    System.out.println(String.format("TERM %d is granting vote to %s   ", b.term,  b.candidateId));
                    cancelTimer("electionTimeout");
                    setTimer("electionTimeout", "timeOut", Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
                    return goTo(Roles.Follower).using(a);
                } else {
                    return stay().using(a);
                }
            } else if (b.term == a.currentTerm && (b.lastLogIndex > a.serverLog.size())) { // else  it does not vote
                //else if the term is the same

                sendBackTermNotice(a, b);
                return stay().using(a);
            } else { // when a.currentterm > b.term
                sendBackTermNotice(a, b);
                return stay().using(a);
            }
        }


        // this happens when a follower receives request to vote for a specific candidate node
        private FSM.State<Roles, NodeData> followerReceiveRequestVoteRPC(NodeData a, RequestVoteRPC b) {
            System.out.println(String.format("    %s inside followerReceiveRequestVoteRPC  %d", getSelf(), b.term));
            //receiveLog(b);
            System.out.println(String.format("line 347  a.currentTerm  %d and b.term is %d  and a.getIfVoted()  %s ", a.currentTerm, b.term, a.getIfVoted()));
            Situation sit = checkSitu(a, b); // check if the request comes from the correct term

            return ifToVote(a, b);
        }

        public void getNodeMessageInfo(NodeData a, SomeMessage b) {
            System.out.println(String.format("the b.term %d and a.currentTerm is %d  a.serverLog.size() is %d ", b.term, a.currentTerm, a.serverLog.size()));
        }

        private FSM.State<Roles, NodeData> followerReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {
            System.out.println(String.format(" %s inside followerReceiveAppendEntryRPC  node term is %d and message term is %d", getSelf(), a.currentTerm,  b.term));
            //receiveLog(b);
            a.displayServerLog();
            Situation situ = checkSitu(a, b);
            if (situ == Situation.same) {
                boolean compareAtoB = a.handleAppendEntryRPC(b);  //

                // reset electionTimeout since it receives AppendEntryRPC
                cancelTimer("electionTimeout");
                setTimer("electionTimeout", "timeOut", Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if the follower is outdated compared to the leader that send the message
                a.updateTerm(a, b);
                return handleOutdatedNode(a, b);

            } else { // if the leader is outdated
                // send message back to prompt it to change
                sendBackTermNotice(a,b);
                return stay().using(a); // do not need to reset timeout
            }
        }


        private FSM.State<Roles, NodeData> followerReceiveHeartBeatRPC(NodeData a, SomeMessage b) {
            System.out.println(String.format("    inside followerReceiveHeartBeatRPC the node term is %d  and message term is %d ", a.currentTerm, b.term));
            //receiveLog(b);

            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                // reset timer
                a.leaderRef = b.messageSender;
                cancelTimer("electionTimeout");
                setTimer("electionTimeout", "timeOut", Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);

                return stay().using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if the follower  is outdated compared the message sender(leader)
                // update its term--> already done
                a.leaderRef = b.messageSender;
                return handleOutdatedNode(a, b);

            } else { // if the  follower is more updated than the leader
                // send a message back --> already done
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }

        private FSM.State<Roles, NodeData> receiveTermMessage(NodeData a, TermMessage b) {
            if (a.currentTerm < b.term) {
                // update term

                a.currentTerm = b.term;
                return goTo(Roles.Follower).using(a);
            } else if (a.currentTerm == b.term && a.serverLog.size() < b.logLength) {
                return goTo(Roles.Follower).using(a);
            } else {
                return stay().using(a);
            }
        }


        private FSM.State<Roles, NodeData> followerReceiveTermMessage(NodeData a, TermMessage b) {
            System.out.println(String.format("    %s inside followerReceiveTermMessage  %d", getSelf(), b.term));
            System.out.println("inside followerReceiveTermMessage");
            return receiveTermMessage(a, b);
        }


        private FSM.State<Roles, NodeData> candidateReceiveTermMessage(NodeData a, TermMessage b) {
            System.out.println(String.format("    %s inside followerReceiveTermMessage  %d", getSelf(), b.term));
            System.out.println("inside followerReceiveTermMessage");
            return receiveTermMessage(a, b);
        }


        private FSM.State<Roles, NodeData> leaderReceiveTermMessage(NodeData a, TermMessage b) {
            System.out.println(String.format("    %s inside followerReceiveTermMessage  %d", getSelf(), b.term));
            System.out.println("inside followerReceiveTermMessage");
            return receiveTermMessage(a, b);
        }


        private FSM.State<Roles, NodeData> followerTimeOut(NodeData a, String b) {
            System.out.println(String.format("   inside followerTimeOut  the node term is %d  and it is turning into candidate  ", a.currentTerm));

            a.incrementTerm();   // increment current term before sending messages
            RequestVoteRPC message = new RequestVoteRPC(a);
            ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
            System.out.println(selection);
            selection.tell(message, getSelf()); // send the RequestResource message to all the selection and specify the sender is this sender
            a.setIfVoted(false);// set just in case
            a.setVoteCount(1); // set just in case , it becomes candidate therefore vote for itself first
            a.currentRole = Roles.Candidate;
            theRole = Roles.Candidate;
            return goTo(Roles.Candidate).using(a);  // stay but reset the timeout
        }


        // receiver: Candidate   only candidate can receive Vote Message
        //              candidate: step down if discover  higher term
        //              leader: step down if currentTerm changes
        //


        /*
        send back a message to ask receiver to update its
         */
        public void sendBackTermNotice(NodeData a, SomeMessage b) { // send
            System.out.println(String.format("    %s inside sendBackTermNotice  %d", getSelf(), b.term));

            TermMessage aMes = new TermMessage(a);
            b.messageSender.tell(aMes, getSelf());
        }





        private FSM.State<Roles, NodeData> candidateReceiveVoteMessage(NodeData a, VoteMessage b) {
            //receiveLog(b);
            System.out.println(String.format("inside candidateReceiveVoteMessage and the node term is %d and message term is %d", a.currentTerm, b.term));
            getNodeMessageInfo(a, b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                ++a.voteCount;
                System.out.println(String.format("Now the voteCount is %d and the total number of members are %d", a.getVoteCount(), a.getTotalNumberMember()));
                if (a.getVoteCount() > a.getTotalNumberMember() / 2) { //if majority votes then it becomes the leader for the current term
                    // send out heartbeat message
                //if (a.getVoteCount() > 1) {

                    System.out.println("I got more than one vote!!!!!!!!!!!!!!!!!!!");
                    HeartBeatRPC heartB = new HeartBeatRPC(a); // make the first heart beat message
                    ActorSelection selection = getContext().actorSelection("/user/*");
                    System.out.println(String.format("!!!!!!!!!!!! won election The message line 491:  %s", selection));
                    selection.tell(heartB, getSelf()); // send heartbeat to everyone else
                    // This will schedule to send the Tick-message
                    // to the tickActor after 0ms repeating every 50ms
                   // System.out.println("line 485");
                    //a.reinitializeNextIndex();


                    // new HeartBeatRPC(a);
                    cancellable = context().system().scheduler()
                            .schedule(Duration.ofMillis(0), Duration.ofMillis(100),
                                    new Runnable() {
                                        @Override
                                        public void run() {
                                            selection.tell(new HeartBeatRPC(a), getSelf());  // send heartbeat
                                            // a.checkLastLogIndexVersusNextIndexAndSendLogs();    // check and see if to send AppendEntryRPC
                                        }
                                    },
                                    context().system().dispatcher());

                    a.currentRole = Roles.Leader;
                    a.ifVoted = false;
                    return goTo(Roles.Leader).using(a);

                } else {   // if the situation is the same
                    return stay().using(a);
                }
            } else if (situ == Situation.exceed) {  // if the candidate  is outdated compared the message sender(follower)

                return handleOutdatedNode(a, b);
                // stay but reset the timeout

            } else { // if the  candidate is more updated than the message sender
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }


        /**
         * @param a
         * @param b
         * @return
         */
        private FSM.State<Roles, NodeData> candidateReceiveHeartBeatRPC(NodeData a, SomeMessage b) {
            //receiveLog(b);

            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                a.leaderRef = b.messageSender;
                // reset timer
                a.currentRole = Roles.Follower;
                a.ifVoted = false;
                return goTo(Roles.Follower).using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if the follower  is outdated compared the message sender(leader)
                // update its term
                a.leaderRef = b.messageSender;
                return handleOutdatedNode(a, b);  // stay but reset the timeout

            } else { // if the  follower is updated
                //
                return stay().using(a); // do not need to reset timeout
            }
        }


        /**
         * @param a
         * @param b
         * @return
         */
        private FSM.State<Roles, NodeData> candidateReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {
            System.out.println(String.format("inside candidateReceiveAppendEntryRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));
            //receiveLog(b);

            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                // as a way to prolong the current timeout
                a.currentRole = Roles.Follower;
                a.ifVoted = false;
                return goTo(Roles.Follower).using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // update its term
                return stay().using(a).forMax(Duration.ofMillis(5));  // stay but reset the timeout

            } else { // if updated
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }


        // this happends when it did not receive heartbeatRPC or receiveFromClientRPC and timeOUT, therefore there's no received message as parameter
        private FSM.State<Roles, NodeData> candidateTimeOut(NodeData a, String lol) {
            // update its term number
            //
            System.out.println(String.format("inside candidateTimeOut the node term is %d ", a.currentTerm));
            a.resetVote(); // just in case, reset the vote count to zero
            a.setVoteCount(1); // vote for itself
            a.setIfVoted(true); // it can't vote anymore in this term
            RequestVoteRPC aCurMes = new RequestVoteRPC(a);  // Message that ask any
            ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
            selection.tell(aCurMes, getSelf());
            cancelTimer("electionTimeout");

            setTimer("electionTimeout", "timeOut", Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)), false);

            //stay().using(a).timeout(Duration.ofMillis(ThreadLocalRandom.current().nextInt(150, 300)));
            return stay().using(a);
        }


        private FSM.State<Roles, NodeData> leaderReceiveAppendEntryRPC(NodeData a, AppendEntryRPC b) {

            System.out.println(String.format("inside leaderReceiveAppendEntryRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));
            a.reinitializeNextIndex(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {

                // do nothing , not going to happen
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if  is outdated compared the message sender(follower)
                // reset its values and step down

                return handleOutdatedNode(a, b);
                // stay but reset the timeout

            } else { // if is more updated than the message sender
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }


        // figure out
        private FSM.State<Roles, NodeData> candidateReceiveRequestVoteRPC(NodeData a, RequestVoteRPC b) {
            System.out.println(String.format("inside candidateReceiveRequestVoteRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));
            //receiveLog(b);
            Situation situ = checkSitu(a, b); // received a Situation message
           // a.updateRef(b);

            return ifToVote(a, b);
        }

        // candidateReceiveRequestVoteRPC--> ignore since only


        //when leader to follower can only happens when it get message with higher term number
        // the term number update will be handled by checkSitu


        private FSM.State<Roles, NodeData> leaderReceiveVoteMessage(NodeData a, SomeMessage b) {
            System.out.println(String.format("inside leaderReceiveVoteMessage the node term is %d and the  message term is %d", a.currentTerm, b.term));
            //receiveLog(b);
            a.reinitializeNextIndex(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {

                // do nothing , not going to happen
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if  is outdated compared the message sender(follower)
                // reset its values and step down

                return handleOutdatedNode(a, b);
                // stay but reset the timeout

            } else { // if is more updated than the message sender
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }




        private FSM.State<Roles, NodeData> handleOutdatedNode(NodeData a, SomeMessage b) {
            //receiveLog(b);
            a.currentTerm = b.term;  //update term
            a.resetVote();
            a.setIfVoted(false);
            a.currentRole = Roles.Follower;
            a.voteCount =0;
            return goTo(Roles.Follower).using(a);
        } // update the term in the node data

        private FSM.State<Roles, NodeData> leaderReceiveHeartBeatRPC(NodeData a, HeartBeatRPC b) {
            //System.out.println(String.format("inside leaderReceiveHeartBeatRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));
            //receiveLog(b);

            a.reinitializeNextIndex(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                // reset timer
                a.leaderRef =  b.messageSender;
                // ignore, we are already leader.
                return stay().using(a);
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                // reset data update its term
                a.leaderRef = b.messageSender;
                return handleOutdatedNode(a, b);  // turn itself to follower
            } else { // if the  follower is updated
                sendBackTermNotice(a, b);
                return stay().using(a); // do not need to reset timeout
            }
        }

        // update lastIndex
        private FSM.State<Roles, NodeData> leaderReceiveAcknowledgementRPC(NodeData a, AcknowledgementMessage b) {
            System.out.println(String.format("inside leaderReceiveAcknowledgementRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));

            a.reinitializeNextIndex(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                // as a way to prolong the current timeout

                a.receiveCheckLastLogIndexVersusNextIndexAndSendLogsResult(b);  // receive aknowledgement message
                a.checkNforCommitIndex(); // update the commit index;

                return stay().using(a); // reset its timer if it receives heart beat from a current term leader
            } else if (situ == Situation.exceed) {  // if situ is outdated compared to the message sender(leader)
                // update its term
                // still need to update the
                return handleOutdatedNode(a, b); // stay but reset the timeout
            } else { // if updated
                return stay().using(a); // ignore
            }
        }

        private FSM.State<Roles, NodeData> rerouteClientMessage(NodeData a, Client.RawMessage b){
            //System.out.println("The leaderRef is " + a.leaderRef);
            //System.out.println("The leaderRef is " + a.leaderRef);
           // if(a.leaderRef!=null) { // if we has leader reference
               // System.out.println("the leaderRef is not null " + a.leaderRef);
                try {
                    a.leaderRef.tell(b, ActorRef.noSender());
                }catch(NullPointerException e){
                    e.printStackTrace();
                }
          //  }else{ // if the leaderRef is not updated ready, then just send the message to every ones
           //     ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
            //   selection.tell(b , getSelf());  // send the RawMessage to all the selected actors.


            //}

            return stay().using(a);
        }


        private FSM.State<Roles, NodeData> leaderReceiveRequestVoteRPC(NodeData a, RequestVoteRPC b) {
            System.out.println(String.format("inside leaderReceiveRequestVoteRPC the node term is %d and the  message term is %d", a.currentTerm, b.term));
            //receiveLog(b);
            a.reinitializeNextIndex(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            return ifToVote(a, b);
        }


        private FSM.State<Roles, NodeData> leaderReceiveFailureMessage(NodeData a, SomeMessage b) {

            //receiveLog(b);
           // a.updateRef(b);
            Situation situ = checkSitu(a, b); // received a Situation message
            if (situ == Situation.same) {
                return stay().using(a); // do nothing since it's already a leader
            } else if (situ == Situation.exceed) {  // if   is outdated compared the message sender(leader)
                return handleOutdatedNode(a, b);// stay but reset the timeout

            } else { // if updated
                return stay().using(a); // do not need to reset timeout
            }
        }

        public FSM.State<Roles, NodeData> leaderReceiveConsistentWriteRequest(NodeData a, Client.RawMessage b){
            // check if write
            System.out.println(String.format("  inside leaderReceiveConsistentWriteRequest the node term is %d and" +
                    " the the remaining ticket number is client is %d  and the message requesting tick number ", a.currentTerm,
                    a.aMachine.ticketNum, b.requestedTick));
           if(b.requestedTick < a.aMachine.ticketNum){  // if the ticket number to reserve is smaller than the ticketNum
                //send back the acknowledgement message
               System.out.println(String.format("   inside leaderReceiveConsistentWriteRequest  and is making  AcknowledgeWriteProcedure  "));
                // add the Entry into the serverLog
               a.serverLog.add(new Entry(b.requestedTick, a.currentTerm, a.serverLog.size()));
               System.out.println(String.format("  line 972 "));
               // requestedTick is 2, currentTerm is 3,  serverLog.size is 4
               // add the entry into the serverlog
               a.checkLastLogIndexVersusNextIndexAndSendLogs(a.ownerActorRef); // check and see if to send AppendEntryRPC
               AcknowledgeWriteProcedure aMes =  new AcknowledgeWriteProcedure(b.requestedTick, b.ticketReserved);
               b.sender.tell(aMes, getSelf());   // send the acknowledgement message to the sender.
           }else{
               // because the number of ticket is not enough anymore, so just send back the
               System.out.println("the requested ticket number is larger than the available ticketnum, therefore it");
               FailureMessage aFailMes = new FailureMessage();
               // aFailMes
               b.sender.tell(aFailMes,getSelf()); // send the failure message to original resource requester
           }

            ////receiveLog(b);
            //System.out.println("line 627");

            return stay().using(a);

        }

        public FSM.State<Roles, NodeData>  ReturnCopyOfStateMachine(NodeData a, Client.UnstableReadRequest b ){
            // return a copy of statemachine  to the
            CopyOfStateMachine acopy = a.aMachine.writeMachine(a,b);
            // return the copy of 
            b.theSender.tell(acopy,getSelf()); // send the CopyOfStateMachine to the sender.
            return stay().using(a);

        }


        // no matter the receiver is candidate, leader, or follower, it always result its data state and change/stay to a follower
        public Situation checkSitu(NodeData a, SomeMessage b) {
            //receiveLog(b);
            Situation situ = a.compare(b.term);   //if the current node has smaller term than the term of the message; then it will generate
           // a.updateRef(b);   // update a reference
            // the models
            System.out.println(String.format("705  in checkSitu the term for node term is %d and the message term is %d", a.currentTerm, b.term));
            if (situ == Situation.same) {

                //System.out.println(String.format("at line 711 before the update function  adn the"));
            } else if (situ == Situation.exceed) {
                // a becomes follower therefore reselt data members
                //System.out.println(String.format("at line 718 before the update function  adn the"));

            } else { // if the original message sender is outdated.
                //System.out.println(String.format("at line 712 before the update function  adn the"));
                // do nothing , as the original message sender will be notified it's outdated eventually
            }
            return situ;
        }   // return a situ


        final LoggingAdapter logHandle = Logging.getLogger(getContext().getSystem(), this);


        //startWith(Candidate, Uninitialized);  // start with candidate state and empty data
        {
            NodeData a = new NodeData(getSelf(), logHandle, 5, 1000);  // start from 1000 ticket
            startWith(Roles.Follower, a); // initialize NodeData associated with the Node
            init(a);
            // System.out.println("line 684");
// it starts to wait for the response of the other grab


            when(Roles.Candidate,
                    matchEventEquals("timeOut",
                            (theMessage, data) -> candidateTimeOut(data, theMessage)).
                            event(AppendEntryRPC.class, NodeData.class,
                                    (theMessage, data) -> candidateReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> candidateReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(VoteMessage.class,
                                    (theMessage, data) -> candidateReceiveVoteMessage(data, VoteMessage.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> candidateReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))).
                            event(TermMessage.class,
                                    (theMessage, data) -> candidateReceiveTermMessage(data, TermMessage.class.cast(theMessage))).
                            event(Client.RawMessage.class,
                                    (theMessage, data) -> rerouteClientMessage(data, Client.RawMessage.class.cast(theMessage))).
                            event( Client.UnstableReadRequest.class,
                                    (tehMessage,data) ->  ReturnCopyOfStateMachine(data, Client.UnstableReadRequest.class.cast(tehMessage))
                            )

            );
            System.out.println("line 716");

            when(Roles.Follower,
                    matchEventEquals("timeOut",
                            (theMessage, data) ->
                                    followerTimeOut(data, theMessage)).
                            event(AppendEntryRPC.class,
                                    (theMessage, data) -> followerReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> followerReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> followerReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))).
                            event(TermMessage.class,
                                    (theMessage, data) -> followerReceiveTermMessage(data, TermMessage.class.cast(theMessage))
                            ).
                           
                            event(Client.RawMessage.class,
                                    (theMessage, data) -> rerouteClientMessage(data, Client.RawMessage.class.cast(theMessage))).
                            event( Client.UnstableReadRequest.class,
                                    (tehMessage,data) ->  ReturnCopyOfStateMachine(data, Client.UnstableReadRequest.class.cast(tehMessage))
                            )
                            //.
                          //  event(VoteMessage.class,
                          //          (theMessage, data) -> follwerReceiveVoteMessage(data, VoteMessage.class.cast(theMessage))
                           // )


            );


            when(Roles.Leader,
                    matchEvent(AppendEntryRPC.class,
                            (theMessage, data) -> leaderReceiveAppendEntryRPC(data, AppendEntryRPC.class.cast(theMessage))).
                            event(HeartBeatRPC.class,
                                    (theMessage, data) -> leaderReceiveHeartBeatRPC(data, HeartBeatRPC.class.cast(theMessage))).
                            event(VoteMessage.class,
                                    (theMessage, data) -> leaderReceiveVoteMessage(data, VoteMessage.class.cast(theMessage))).
                            event(RequestVoteRPC.class,
                                    (theMessage, data) -> leaderReceiveRequestVoteRPC(data, RequestVoteRPC.class.cast(theMessage))).
                            event(Client.RawMessage.class,
                                    (theMessage, data) -> leaderReceiveConsistentWriteRequest(data, Client.RawMessage.class.cast(theMessage))).
                            event(AcknowledgementMessage.class,
                                    (theMessage, data) -> leaderReceiveAcknowledgementRPC(data, AcknowledgementMessage.class.cast(theMessage))).
                            event(TermMessage.class,
                                    (theMessage, data) -> leaderReceiveTermMessage(data, TermMessage.class.cast(theMessage))).
                            event( Client.UnstableReadRequest.class,
                                    (tehMessage,data) ->  ReturnCopyOfStateMachine(data, Client.UnstableReadRequest.class.cast(tehMessage))
                            )
                            // if you ConsistentWriteRequest
            );


            onTransition(
                    matchState(Roles.Leader, Roles.Follower,
                            () -> {
                                // System.out.println("line 750");
                                cancelTimer("electionTimeout");
                                cancellable.cancel();  // cancel the timer
                            }). // set follower timeout
                            state(Roles.Candidate, Roles.Follower,
                            () -> {
                                System.out.println("Candidate become follower ");
                            }).
                            state(Roles.Candidate, Roles.Leader, () -> {
                                cancelTimer("electionTimeout");

                            }).  // leader don't have timer
                            state(Roles.Follower, Roles.Candidate,
                            (f, t) -> {
                                cancelTimer("electionTimeout"); // reset timer
                                int random = ThreadLocalRandom.current().nextInt(150, 300);
                                setTimer("electionTimeout", "timeOut", Duration.ofMillis(random), true);

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
            logHandle.info("{} received  {} ", getSelf(), m.toString());

        }
/*
        public void sendLog(SomeMessage m) {
            if (m instanceof RequestResource) {
                this.serverLog.info("{} sent *** Request Resource message: {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            } else if (m instanceof ReleaseResource) {
                this.serverLog.info("{} sent *** Release Resource messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            } else if (m instanceof Aknowledgement) {
                this.serverLog.info("{} sent *** Aknowledgement messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
            }
        }

        */
    }


}