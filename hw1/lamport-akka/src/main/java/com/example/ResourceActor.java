package com.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.*;

//import akka.dispatch.BoundedMessageQueueSemantics;
//import akka.dispatch.RequiresMessageQueue;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.actor.*;


import java.lang.*;

import akka.actor.Terminated;


public class ResourceActor extends AbstractActor {

    static Props props() {
        return Props.create(ResourceActor.class, ResourceActor::new);
    }

    private long logicalCounter = 0;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private List<RequestResource> requestmessageQueue = new ArrayList<RequestResource>();

    public void syncLogicalTimeUponReceival(long messageTimeStamp) {
        if (this.logicalCounter > messageTimeStamp) {
            this.logicalCounter = this.logicalCounter + 1;
        } else {
            this.logicalCounter = messageTimeStamp + 1;
        }

    }




    // Message classes
    static public abstract class SomeMessage {
        private  final ActorRef from;
        private  final  String theMessage;
        private  final long timeStamp;

        SomeMessage(ActorRef from, String theMessage, long currentLogicalCounter) {
            this.from = from;
            this.theMessage = theMessage;
            this.timeStamp = currentLogicalCounter + 1;
        }
        public String getTheMessage(){
            return(theMessage);
        };


        public  long getTimeStamp(){
            return timeStamp;
        };

        public  ActorRef getMessenger(){
            return from;
        };
    }

    static public class RequestResource extends SomeMessage implements Comparable<RequestResource> {

        public RequestResource(ActorRef from, String theMessage, long currentLogicalCounter) {
                super(from,theMessage,currentLogicalCounter );

        }
        public int compareTo(RequestResource anotherInstance) {
            int diff = (int) (this.getTimeStamp() - anotherInstance.getTimeStamp());
            return diff;
        }

    }

    static public class ReleaseResource extends SomeMessage {
        private final long originalRequestTimeStamp;


        public ReleaseResource(ActorRef from, String theMessage, long originalRequestTimeStamp, long currentLogicalCounter) {
            super(from,theMessage,currentLogicalCounter );
            this.originalRequestTimeStamp = originalRequestTimeStamp;

        }

        public long getOriginalRequestTimeStamp() {
            return originalRequestTimeStamp;
        }

    }

    static public class Aknowledgement extends SomeMessage {

        private final long requestResourceTimeStamp;

        public Aknowledgement(ActorRef from, String theMessage, long requestResourceTimeStamp, long currentLogicalCounter) {
            super(from, theMessage, currentLogicalCounter);
            this.requestResourceTimeStamp = requestResourceTimeStamp;

        }


        public long getRequestResourceTimeStamp() {
            return requestResourceTimeStamp;
        }

    }



    // log methods

    public void receiveLog(SomeMessage m) {
        if (m instanceof RequestResource) {
            this.log.info("{} received Request Resource message: {}:{}  at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        } else if (m instanceof ReleaseResource) {
            this.log.info("{} received Release Resource messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        } else if (m instanceof Aknowledgement) {
            this.log.info("{} received Aknowledgement messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        }
    }

    public void sendLog(SomeMessage m) {
        if (m instanceof RequestResource) {
            this.log.info("{} sent *** Request Resource message: {}:{}  at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        } else if (m instanceof ReleaseResource) {
            this.log.info("{} sent *** Release Resource messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        } else if (m instanceof Aknowledgement) {
            this.log.info("{} sent *** Aknowledgement messages: {}:{} at its own logical clock {}", getSelf(), m.getMessenger(), m.getTimeStamp(), this.logicalCounter);
        }
    }




    // if the queue is "Tm:P(ifself) request resource" message and Tm is smaller than all other Tm s in the message queue,
    //      then it successfully requested message by printing out a message, then it remove the "Tm:P(itself) request resource" message
    //      from its mailbox, and send "Tm:P(itself) release resource" message to all other actors

    public void checkQueue(long timeStamp) {
        if (this.requestmessageQueue != null) {
            //System.out.println("the requestMessageQueue is not null");
            if (this.requestmessageQueue.size() > 0) {
                //System.out.println("line 136");
                RequestResource theTop = this.requestmessageQueue.get(0);
                //System.out.println(theTop);
                if (theTop.getMessenger() == getSelf() // if the request message sender is itself
                        && theTop.getTimeStamp() < timeStamp) { // the request message timestamp is earlier than any of the new messages coming from the mailbox

                    // send ReleaseResource message to all the other actors
                    ReleaseResource releaseMessage = new ReleaseResource(getSelf(), "Release Resource", theTop.getTimeStamp(), logicalCounter);  // construct the ReleaseResource message with the timeStamp of the RequestResource message as well.
                    ActorSelection selection = getContext().actorSelection("/user/*");  // get all the other actors in the system
                    selection.tell(releaseMessage, getSelf()); // send the RequestResource message to all the selection and specify the sender is this sender

                    // remove the ReleaseResource message from the queue in itself
                    this.requestmessageQueue.remove(0); // remove the request from the queue once the resource has been granted
                    log.info("{} is granted resource ", getSender());

                    sendLog(releaseMessage);
                }
            }
        }
    }




    // insert  the RequestResource to the RequestResource messages queue based on its timestamp, the queue is first in first out.
    static public void insertMessage(List<RequestResource> queue, RequestResource x) {
        if (queue.size() == 0) {
            queue.add(x);
        } else {
            int queuesize =queue.size();
            for (int i = 0; i < queuesize; i++) {
                if (queue.get(i).getTimeStamp() > x.getTimeStamp()) {
                    queue.add(i, x);
                }
            }
        }
    }


    //remove RequestResource message when its sender and timestamp matches the sender of ReleaseResource and its requestResource timestamp
    static public void removeRequestFromReleaseMessage(List<RequestResource> requestmessageQueue, ReleaseResource m, ResourceActor act) {
        for (int i = 0; i < requestmessageQueue.size(); i++) {
            if (requestmessageQueue.get(i).getMessenger() == m.getMessenger() &&
                    requestmessageQueue.get(i).getTimeStamp() == m.getOriginalRequestTimeStamp()) { // the ReleaseResource message has timestamp of the original RequestResource message
                act.log.info("{} Removed RequestResource message from queue, the message is {}:{}", act.getSelf(), requestmessageQueue.get(i).getMessenger(), requestmessageQueue.get(i).getTimeStamp());
                requestmessageQueue.remove(i); // remove from the request queue.
            }
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("done", m -> getContext().stop(getSelf()))  // this stops the actor ifself
                .match(String.class,


                        m -> {
                            log.info("Received String message: {}", m);
                           // System.out.println("A string message received");
                        })
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
                .match(PoisonPill.class, m -> {    // extension part of the homework
                    Throwable es = new ActorKilledException("Target actor terminated.");
                    getSender().tell(new Status.Failure(es), getSelf());
                    getContext().stop(getSelf());  // let the actor stop itself
                })
                .build();
    }











}

