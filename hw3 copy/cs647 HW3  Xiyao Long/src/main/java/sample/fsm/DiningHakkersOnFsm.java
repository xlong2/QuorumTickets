package sample.fsm;

import java.io.IOException;

import akka.actor.ActorSystem;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import akka.actor.*;

import java.io.Console;
import java.io.BufferedReader;

import akka.actor.Props;

import java.io.IOException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import static sample.fsm.Messages.*;


public class DiningHakkersOnFsm {


    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create();

        int num = 10;
        int clientNum = 3;
        ActorRef[] nodes = new ActorRef[num];
        ActorRef[] clients = new ActorRef[clientNum];

        for (int i = 0; i < num; i++)
            nodes[i] = system.actorOf(Props.create(NodeOnFsm.Node.class), "node" + i);

        ActorSelection selection = system.actorSelection("/user/*");  // get all the other actors in the system
        for (int i = 0; i < clientNum; i++) {
            ActorRef aclient = system.actorOf(Props.create(Client.class), "Clientnode" + i);
            clients[i]=aclient;
            String clientM = String.format("ClientMessage%d" , i);
            system.scheduler().schedule(Duration.ofMillis(0), Duration.ofMillis(100),
                            new Runnable() {
                                @Override
                                public void run() {
                                    selection.tell(new Client.RawMessage(clientM),aclient) ;  // send heartbeat
                                }
                            },
                            system.dispatcher());
        }


        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }


    }
}