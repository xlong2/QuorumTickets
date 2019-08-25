package sample.fsm;

import static sample.fsm.Messages.*;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Client extends AbstractActor {
    // the list contains acknowledgement messages
    static Props props() {


        return Props.create(Client.class, Client::new);
    }

    public static class GetAlive {
        String a;
        public GetAlive() {
            a = "just to make clients alive";
        }}

    public Cancellable aCancellable;
    public int availableTicNum ;
    ArrayList<BuyTickRequest> messageList;
    private static long messageCounter ;
    public ActorRef backendNode;
    public int reservedTicNum ;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public  Client( ){
         // availableTicNum = tickNum ;
        messageList = new ArrayList<>();
        reservedTicNum=0;
          messageCounter = 1;
        //log = Logging.getLogger(getContext().getSystem(), this);

    }



    public static class BuyTickRequest{
        int tickToBuy;
        ActorRef sender;
        public BuyTickRequest(ActorRef thesender, int num ){
            tickToBuy = num;
            sender = thesender;
        }
    }



    public static class RawMessage {
        public String message;
        public int requestedTick;    // the number to ticket to buy
       public  ActorRef sender;
       public int ticketReserved;
        public RawMessage(String mess, int ticketNum2Buy, ActorRef theSender, int resNum) {

            requestedTick = ticketNum2Buy; //
            ++messageCounter;
            sender = theSender;
            message = "ClientMessage" + messageCounter;
            ticketReserved = resNum;
        }

        @Override
        public String toString() {
            return message;
        }

    }

    // when try to buy a ticket, send a request StateMachine message request and send a message
    // upon receiving a message,  get the remaining ticket number and
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("done", m -> getContext().stop(getSelf()))  // this stops the actor ifsel
                .match( // treat the wrong
                        UpdateBackendNode.class,   // when the node knows the corresponding backend node reference.
                         m -> {
                            // corresponding actor reference
                            ActorRef correspondingRef = m.backendRef;
                            // update the corresponding backendnode
                            backendNode = correspondingRef; // the backendNode is
                            context().system().scheduler()
                                    .schedule(Duration.ofMillis(5000), Duration.ofMillis(1000),
                                            new Runnable() {
                                                @Override
                                                public void run() {
                                                    // it sends unstable read request
                                                    backendNode.tell(new UnstableReadRequest(getSelf()), getSelf());  // send read request to the corresponding endnode backendNode
                                                    // a.checkLastLogIndexVersusNextIndexAndSendLogs();    // check and see if to send AppendEntryRPC
                                                }
                                            },
                                            context().system().dispatcher());

                            // simulate when people buy tickets in random periods

                             aCancellable = context().system().scheduler()
                                     .schedule(Duration.ofMillis(8000), Duration.ofMillis(100),
                                             new Runnable() {
                                                 @Override
                                                 public void run() {
                                                     int random = (int) (Math.random() * 5) + 1; // get the random number
                                                     // it sends unstable read request
                                                     getSelf().tell(new BuyTickRequest(getSelf(), random), getSelf());  // send read request to the corresponding endnode backendNode
                                                     // a.checkLastLogIndexVersusNextIndexAndSendLogs();    // check and see if to send AppendEntryRPC
                                                 }
                                             },
                                             context().system().dispatcher());



                        }
                        )
                .match(CopyOfStateMachine.class,   // this ReturnCopyStateMachine should have the number of ticket that initially asks for
                        // the backend will return a UnstableReadRequest as a result.
                        m -> {
                                // the number of
                                availableTicNum  = m.ticketNum;  // the number of ticket number is  updated from the ticket number from the message

                               // ActorSystem system = ActorSystem.create();
                               // ActorSelection selection = system.actorSelection("/user/*");  // get all the other actors in the system
                                //selection.tell(new RawMessage("a message", 12 ), getSelf());
                                // client Broadcast the RawMessage to ensure that a a leader gets such message
                                //return the send message information.
                                System.out.println(String.format("****** The ticket number remaining from unstable reads is %d  from ****** ", availableTicNum));
                            
                        }

                )
                // when client received a reserve ticket request ticket message by itself,
                .match(BuyTickRequest.class,  // get the BuyTickRequest.class
                        m->{
                            System.out.println(String.format("inside match(BuyTickRequest.class and tickToBuy: %d and reservedTicNum: %d ", m.tickToBuy, reservedTicNum));
                            if(m.tickToBuy < reservedTicNum){   //
                                reservedTicNum = reservedTicNum -m.tickToBuy;
                                messageList.add(m);// add the
                            }else{
                                System.out.println(String.format("The ticket number is not enough, client is requesting tickets from node leader "));

                               // if(  availableTicNum< 100){
                                 //   RawMessage aRequest = new RawMessage("theMes", m.tickToBuy, getSelf(),m.tickToBuy );
                                  //  backendNode.tell(aRequest, getSelf()); // send messages to the corresponding backend
                                //}else {
                                System.out.println(" the availableTicNum "+availableTicNum);
                                    RawMessage aRequest = new RawMessage("theMes", 20, getSelf(), m.tickToBuy);
                                    backendNode.tell(aRequest, getSelf()); // send messages to the corresponding backend



                                //}
                                }
                        })
                .match(AcknowledgeWriteProcedure.class,  //
                        m->{
                             messageList.add(new BuyTickRequest(getSelf(),m.ticketReservedByMotivator));
                             reservedTicNum =  reservedTicNum +  m.ticketNumReserved- m.ticketReservedByMotivator;  // the number of reserved Tic Num in the client node
                            // add the reservedTicNum  to the ;
                        }

                )
                .match(FailureMessage.class,
                        m->{
                            System.out.println("System does not have that many tickets!!");
                            aCancellable.cancel();
                        }

                )
                /*   // if themessage is a statemachine returned by the nodeData, then use the statemachien to decide if
                .match(
                        RequestResource.class,
                        m -> {
                            syncLogicalTimeUponReceival(m.getTimeStamp());
                            receiveLog(m);


                            if (m.getMessenger() == null) {  // if it comes from system then it send a request message to itself and also send to messages to all other actors
                                // System.out.println("at line 171");
                                RequestResource message = new RequestResource(getSelf(), "requestResource", logicalCounter);
                                ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
                                selection.tell(message, getSelf()); // send the RequestResource message to all the selection and specify the sender is this sender
                                sendLog(message);
                                //System.out.println("at line 176");

                            } else if (m.getMessenger() == getSelf()) { // if the message sender is itself
                                // place the message into a queue
                                //System.out.println(" at line 177");
                                //requestmessageQueue.add(m);
                                insertMessage(requestmessageQueue, m);
                            } else {  // if it does not come from itself nor the system then send back an aknowledgement message
                                //System.out.println("at line 180");
                                // if it come from other actors
                                //requestmessageQueue.add(m);  //still place it into the request array
                                //log.info("the size of the queue is {}", requestmessageQueue.size());
                                insertMessage(requestmessageQueue, m);
                                Aknowledgement akMessage = new Aknowledgement(getSelf(), "add aknowledgement", m.getTimeStamp(), logicalCounter);
                                m.getMessenger().tell(akMessage, getSelf()); // send an aknowledgement message back
                                sendLog(akMessage);

                            }
                            checkQueue(m.getTimeStamp());

                        })
                .match(ReleaseResource.class,   // assume the ReleaseResource message will always come after the corresponding
                        // requestResource message into the mailbox for all the processes, so the requestResource will always exist in the queue waiting for deletion


                        m -> {
                            syncLogicalTimeUponReceival(m.getTimeStamp());

                            removeRequestFromReleaseMessage(requestmessageQueue, m, this);

                            checkQueue(m.getTimeStamp());

                        }

                )
                .match(Aknowledgement.class,

                        m -> {
                            syncLogicalTimeUponReceival(m.getTimeStamp());
                            checkQueue(m.getTimeStamp());
                        }

                )
                */
                .match(PoisonPill.class, m -> {    // extension part of the homework
                    Throwable es = new ActorKilledException("Target actor terminated.");
                    getSender().tell(new Status.Failure(es), getSelf());
                    getContext().stop(getSelf());  // let the actor stop itself
                })
                .build();
    }


    // Read what's the current ticket number;
    public static final class UnstableReadRequest extends Messages.SomeMessage {
        ActorRef theSender;

        public UnstableReadRequest(ActorRef theRef) {
            //
            theSender = theRef;   // the message has the sender ref
        }
    }


    public void sendLog(RawMessage m) {
        this.log.info("Client {} sent the message {}  :  {} ", getSelf(), this.messageCounter, m.toString());
    }


    //remove RequestResource message when its sender and timestamp matches the sender of ReleaseResource and its requestResource timestamp

}
