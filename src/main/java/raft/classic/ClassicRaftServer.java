package raft.classic;

import raft.common.*;
import raft.messaging.common.ControlMessage;
import raft.messaging.common.ControlMessageType;
import raft.messaging.internal.AppendEntries;
import raft.messaging.common.RaftMessage;
import raft.messaging.internal.RequestVote;
import raft.network.Node;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ClassicRaftServer extends RaftServer {

    private int currentTerm;
    private Node<RaftMessage> votedFor;
    private RaftLog log;

    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
        log = new RaftLog();
        currentTerm = 0;
    }

    @Override
    public void runRaft() {
        while(true) {
            try {
                RaftMessage message = getNextMessage();
                handleMessage(message);
            }
            catch (Exception e) {
                System.out.printf("[ERR] (%s)\t Error while running raft:\n", Thread.currentThread().getName());
                e.printStackTrace();
                continue;
            }
        }
    }

    private void handleMessage(RaftMessage message) throws NoSuchMethodException {
        if (message.getControlMessage() != null) {
            handleControlMessage(message.getControlMessage());
        }
        else if (message.getAppendEntries() != null) {
            handleAppendEntries(message.getAppendEntries());
        }
        else if (message.getRequestVote() != null) {
            handleRequestVote(message.getRequestVote());
        }
        else {
            throw new NoSuchMethodException("Message has no content, cannot find matching handler.");
        }
    }

    private void handleAppendEntries (AppendEntries request){

    }

    private void handleRequestVote (RequestVote request){

    }

    private void handleControlMessage (ControlMessage request) {

    }

}
