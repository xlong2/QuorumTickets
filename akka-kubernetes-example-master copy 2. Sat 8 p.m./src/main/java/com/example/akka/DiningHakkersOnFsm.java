package com.example.akka;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.stream.ActorMaterializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.akka.Messages.UpdateBackendNode;
import com.typesafe.config.*;
//import akka.discovery.Discovery;
//import akka.discovery.ServiceDiscovery;

//package com.example.akka;


public class DiningHakkersOnFsm {
    private static final String CLUSTER_NAME = "ClusterSystem";

    public static void main(String[] args) throws IOException {


       // Config conf = ConfigFactory.load("application");
        ActorSystem actorSystem = ActorSystem.create(CLUSTER_NAME);
        // actorSystem.actorOf(SimpleClusterListener.props());
        final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);
        Cluster cluster = Cluster.get(actorSystem);
        // the seed nodes just sit around and do nothing.
        System.setProperty("NODE_TYPE","Client");
        if(System.getenv().get("NODE_TYPE").equals("Client")){
           // actorSystem.actorOf(SimpleClusterListener.props());
            ActorRef aclient = actorSystem.actorOf(Props.create(Client.class), "frontendNode"+System.getenv().get("CLIENTINDEX"));
           // ServiceDiscovery serviceDiscovery = Discovery.get(actorSystem).discovery();
            //ActorSelection noderef = actorSystem.actorSelection("akka://actorSystem@localhost:2522/user/backendNode"+System.getenv().get("CLIENTINDEX"));
            //Future remoteActor = noderef.resolveOne( FiniteDuration.create(5, "seconds"));

            // this actorselection just corresponds to one actorRef
           // UpdateBackendNode theNode = new UpdateBackendNode(noderef); // include the backend node information into the corre

            //aclient.tell(theNode, ActorRef.noSender());

        }else if(System.getenv().get("NODE_TYPE").equals("Backend_node")){
            try {
                Thread.sleep(2000);
            }catch (Exception e){
                e.printStackTrace();
            }
            //actorSystem.actorOf(SimpleClusterListener.props());
            actorSystem.actorOf(Props.create(NodeOnFsm.Node.class), "backendNode" +  System.getenv().get("BACKENDINDEX"));

        }

        List<Address> addresses = Arrays.asList(System.getenv().get("SEED_NODES").split(",")).stream()
                .map(ip -> new Address("akka", CLUSTER_NAME, ip, 2551))
                .collect(Collectors.toList());

        cluster.joinSeedNodes(addresses);  //This is the last ide
    }
}

