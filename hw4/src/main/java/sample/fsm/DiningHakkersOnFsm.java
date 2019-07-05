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



//package com.example.akka;

import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.cluster.Cluster;
import akka.stream.ActorMaterializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.cluster.Cluster;
import akka.stream.ActorMaterializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SimpleClusterMain {
    private static final String CLUSTER_NAME = "ClusterSystem";

    public static void main(String[] args) throws IOException {
        ActorSystem actorSystem = ActorSystem.create(CLUSTER_NAME);
        // actorSystem.actorOf(SimpleClusterListener.props());
        final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);

        Cluster cluster = Cluster.get(actorSystem);
        if(System.getenv().get("NODE_TYPE").equals("Client")){
           // actorSystem.actorOf(SimpleClusterListener.props());
            ActorRef aclient = actorSystem.actorOf(Props.create(Client.class), "Clientnode" + i);

            UpdateBackendNode theNode = new UpdateBackendNode(nodes[i]); // include the backend node information into the corre

            aclient.tell(theNode, ActorRef.noSender());

        }else if(System.getenv().get("NODE_TYPE").equals("Backend_node")){
            //actorSystem.actorOf(SimpleClusterListener.props());
            actorSystem.actorOf(Props.create(NodeOnFsm.Node.class), "node" + i);

        }
        List<Address> addresses = Arrays.asList(System.getenv().get("SEED_NODES").split(",")).stream()
                .map(ip -> new Address("akka.tcp", CLUSTER_NAME, ip, 2551))
                .collect(Collectors.toList());
        cluster.joinSeedNodes(addresses);  //This is the last ide
    }
}


public class DiningHakkersOnFsm {


    public static void main(String[] args) {




        ActorSystem system = ActorSystem.create();

        int num = 5;
        int clientNum = 5;
        ActorRef[] nodes = new ActorRef[num];
        ActorRef[] clients = new ActorRef[clientNum];

        for (int i = 0; i < num; i++)
            nodes[i] = system.actorOf(Props.create(NodeOnFsm.Node.class), "node" + i);

        ActorSelection selection = system.actorSelection("/user/*");  // get all the other actors in the system
        for (int i = 0; i < clientNum; i++) {
            ActorRef aclient = system.actorOf(Props.create(Client.class), "Clientnode" + i);
            clients[i]=aclient;
            String clientM = String.format("ClientMessage%d" , i);
            UpdateBackendNode theNode = new UpdateBackendNode(nodes[i]); // include the backend node information into the corre
            aclient.tell(theNode, ActorRef.noSender());

            // schedule the messages to be sent by the clients periodically
            /*
            system.scheduler().schedule(Duration.ofMillis(0), Duration.ofMillis(10),
                    new Runnable() {
                        @Override
                        public void run() {
                            selection.tell(new Client.RawMessage("a"),aclient) ;  // send heartbeat
                        }
                    },
                    system.dispatcher());

            //schedule the messages to be sent to the clients,



            system.scheduler().schedule(Duration.ofMillis(0), Duration.ofMillis(10),
                            new Runnable() {
                                @Override
                                public void run() {
                                    selection.tell(new Client.RawMessage(clientM),aclient) ;  // send heartbeat
                                }
                            },
                            system.dispatcher());
            */
        }try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        
        for(int i=0; i<clientNum; ++i){
            
        }


    }
}