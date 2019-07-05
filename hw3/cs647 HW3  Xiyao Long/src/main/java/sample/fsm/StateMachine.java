package sample.fsm;

import java.util.HashMap;

public class StateMachine {
    int ticketNum;

    public StateMachine(int totalNum) {
        ticketNum = totalNum;
    }

    public void increment() {
        ++ticketNum;
    }

    public void decrement() {
        --ticketNum;
    }

}
