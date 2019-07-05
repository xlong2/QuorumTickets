package com.example;
import java.io.IOException;

import akka.actor.ActorSystem;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import akka.actor.*;
import java.io.Console;
import java.io.BufferedReader;
import akka.actor.Props;
import java.io.IOException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class LamportClock{
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("lamport");  // create an actor system
        Console console = System.console();
        int nProcess =0;
        try{
            //nProcess = Integer.parseInt(console.readLine("Enter number of process:  "));
            nProcess=5;

        } catch (NumberFormatException e) {


        }

        ActorRef systemActor =  system.actorOf(Props.create(ResourceActor.class));//.withMailbox("prio-mailbox"));

        ActorRef refArray[];
        refArray = new ActorRef[nProcess];

        try {

            // create array of actors
            for (int i =0;i<nProcess;i++) {
                refArray[i]= system.actorOf(Props.create(ResourceActor.class));//.withMailbox("prio-mailbox"));
            }

            for (int i =0;i<nProcess;i++){
                // a message instance where the sender is the systemActor
                ResourceActor.RequestResource a= new ResourceActor.RequestResource(ActorRef.noSender(), "requestResource",i); // use the counter as a logical time
                refArray[i].tell(a, ActorRef.noSender());   // ask the system actor to send request rescource message to each actor in the actor array
            }

            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();

            // extension part of the homework
            // send termination message to all the actors
            system.actorSelection("/user/*").tell("done",ActorRef.noSender());
            system.actorSelection("/user/*").tell(PoisonPill.getInstance(),ActorRef.noSender());

        } catch (IOException ioe) {
        } finally {
            system.terminate();
        }
    }
}