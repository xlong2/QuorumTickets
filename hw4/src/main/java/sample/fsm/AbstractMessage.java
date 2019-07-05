package sample.fsm;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import java.util.Random;

import java.util.*;

//import akka.dispatch.BoundedMessageQueueSemantics;
//import akka.dispatch.RequiresMessageQueue;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.actor.*;
import java.util.concurrent.ThreadLocalRandom;

import java.util.Timer;
import java.util.TimerTask;


import java.lang.*;
/*
send messages to leader
 */
public interface AbstractMessage {};
// Message sent by client



