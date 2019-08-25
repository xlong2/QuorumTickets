package com.example;
import java.io.IOException;

import akka.actor.ActorSystem;
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
import  static com.example.Messages.*;


public class LamportClock{


        public static void main(String[] args) {
            ActorSystem system = ActorSystem.create();

            int num = 2;
            ActorRef[] nodes = new ActorRef[num];
            for (int i = 0; i < num; i++)
                nodes[i] = system.actorOf(Props.create(NodeOnFsm.Node.class), "node" + i);
            try {
                TimeUnit.SECONDS.sleep(1);
            }catch(InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                }



            int n = 0;
            for (ActorRef aNode : nodes) {

                // send a random message contain an Entry to the leader
                String message = String.format("A raw message %d", n);
                n = n + 1;
                RawMessage amess = new RawMessage(message);
                aNode.tell(amess, ActorRef.noSender());


            }
        }

}