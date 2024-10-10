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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class ClassicRaftServer extends RaftServer {

    private final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(1000);
    private int currentTerm;
    private Node<RaftMessage> votedFor;
    private RaftLog log;
    private int id;

    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
        log = new RaftLog();
        currentTerm = 0;
    }

    @Override
    public void runRaft() {
        scheduleHeartbeatMessages();
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
            handleControlMessage(message);
        }
        else if (message.getAppendEntries() != null) {
            handleAppendEntries(message);
        }
        else if (message.getRequestVote() != null) {
            handleRequestVote(message);
        }
        else {
            throw new NoSuchMethodException("Message has no content, cannot find matching handler.");
        }
    }

    private void handleAppendEntries (RaftMessage message){
        System.out.printf("%s from %s\n", message.getAppendEntries(), getInetSocketAddress());
    }

    private void handleRequestVote (RaftMessage message){

    }

    private void handleControlMessage (RaftMessage message) {
        System.out.printf("[i] Received control message from %s: %s.\n", message.getSender().getInetSocketAddress(), message.getControlMessage());
        switch (message.getControlMessage().type()) {
            case HELLO_SERVER -> {
                servers.add(message.getSender());
                clients.remove(message.getSender());
            }
            case HELLO_CLIENT -> {
                clients.add(message.getSender());
                servers.remove(message.getSender());
            }
            case null, default -> System.out.printf("RaftMessage with SEQ %d has unimplemented control type.\n", message.getSequenceNr());
        }

    }

    private void scheduleHeartbeatMessages() {
        timeoutTimer.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        queueServerBroadcast(new RaftMessage(
                                new AppendEntries(
                                        currentTerm,
                                        id,
                                        log.lastApplied,
                                        log.get(log.lastApplied).term(),
                                        new ArrayList<LogEntry>(0),
                                        log.committedIndex
                                )));
                    }
                },
                0,
                HEARTBEAT_INTERVAL.toMillis()
        );
    }

}
