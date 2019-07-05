package sample.fsm;


import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Client extends AbstractActor {

    static Props props() {

        return Props.create(Client.class, Client::new);
    }

    public static class GetAlive{
        String a;
        public GetAlive(){
            a="just to make clients alive";
        }


    }

    private long messageCounter =1;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public void sendMessageToLeaders(){
        context().system().scheduler()
                .schedule(Duration.ofMillis(0), Duration.ofMillis(123),
                        new Runnable() {
                            @Override
                            public void run() {
                                getContext().actorSelection("/user/*").tell(new RawMessage(String.format(" %d and message %d",messageCounter ,messageCounter )), getSelf());  // send heartbeat

                                    // check and see if to send AppendEntryRPC
                            }
                        },
                        context().system().dispatcher());
    }
    public static class  RawMessage {
        public String message;

        public RawMessage(String mess) {
            message = mess;
        }

        @Override
        public String toString() {
            return message;
        }

    }







    public void sendLog(RawMessage m) {
            this.log.info("Client {} sent the message {}  :  {} ", getSelf(), this.messageCounter, m.toString());
    }





    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("done", m -> getContext().stop(getSelf()))  // this stops the actor ifself
                .build();
    }






    //remove RequestResource message when its sender and timestamp matches the sender of ReleaseResource and its requestResource timestamp

}
