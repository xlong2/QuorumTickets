package com.example.akka;

import java.io.Serializable;

public class Entry implements Serializable {
    /*
    when the entry is sent as a ticketNumTaken:
    @para ticketNumTaken : any ticketNumTaken in this log entry
    @termIndex  the termIndex where the ticketNumTaken exists in sender
    @logIndex the index of the ticketNumTaken log
     */
    int ticketNumTaken; // this message can be any
    int termIndex;
    int logIndex;




    public Entry(int ticketNumTaken, int termIndex, int logIndex ){
        this.ticketNumTaken = ticketNumTaken;
        this.termIndex = termIndex;
        this.logIndex = logIndex;
    }

    public Entry( int termIndex, int logIndex ){  // this is an empty entry
        this.ticketNumTaken = 0;
        this.termIndex = termIndex;
        this.logIndex = logIndex;
    }
    public String toString(){
        return  String.format("message %d |termIndex %d | logIndex %d", ticketNumTaken, termIndex, logIndex);
    }

    public int getTerm(){ return termIndex;}
    public int getIndex(){ return logIndex; }

    public int getticketNumTaken(){
        return this.ticketNumTaken;
    }
    public void setTermIndex(int termI){
        this.termIndex = termI;
    }
    public int getTermIndex(){ return this.termIndex; }

    public void setLogIndex(int logI){
        this.logIndex = logI;
    }
    public int getLogIndex( ){ return this.logIndex; } // this




    public Situation unitCompareA2B(int A, int B){
        int diff = A-B;
        if (diff < 0) // the term of sent node is smaller than the current term
        {
            return Situation.exceed; // B has larger number than A
        }else if(diff >0){
            return Situation.behind; // B has smaller number than A
        }else{
            return Situation.same;  // B has the same number as A
        }
    }

    /*
        only compare term number between two entries

        @para termCompare : which entry has a larger  log term
                            if entry2 has a more recent log term it will return Situation.exceed
     */
    public  Situation compareTo1(int termB) {
        // compare if the ticketNumTaken sender term number is larger than the receiver

        Situation termCompare = unitCompareA2B(this.getTermIndex(), termB);

        return termCompare;
    }




}
