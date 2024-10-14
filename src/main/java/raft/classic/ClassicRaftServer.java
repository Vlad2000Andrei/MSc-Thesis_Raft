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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;
import java.util.TimerTask;

public class ClassicRaftServer extends RaftServer {

    // State for this implementation
    private final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(5000);
    private final Duration ELECTION_TIMEOUT_MIN = Duration.ofMillis(150);
    private final Duration ELECTION_TIMEOUT_MAX = Duration.ofMillis(500);
    private Duration electionTimeout = Duration.ofMillis(new Random().nextInt((int)ELECTION_TIMEOUT_MIN.toMillis(), (int)ELECTION_TIMEOUT_MAX.toMillis())); // TODO make this not ugly:
    private final Duration MSG_RETRY_INTERVAL = Duration.ofMillis(50);
    private TimerTask heartbeatTimer;
    private int currentElectionVotes;

    // State from Paper
    private int currentTerm;
    private Node<RaftMessage> votedFor;
    private RaftLog log;
    protected ServerRole role;


    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
        log = new RaftLog();
        currentTerm = 0;

        role = ServerRole.FOLLOWER;
        lastLeaderContact = Instant.now();
        heartbeatTimer = null;
    }

    @Override
    public void runRaft() {
        while(true) {
            try {
                // Check for election timeouts
                if (checkElectionTimeout()) {
                    startElection();
                }

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
        System.out.printf("[Vote] Server %d received RequestVote from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getRequestVote());
        if (role == ServerRole.FOLLOWER || role == ServerRole.LEADER) votedFor = null;

        int candidateTerm = message.requestVote.term();
        int candidateLogSize = message.requestVote.lastLogIndex();
        int candidateLogLastTerm = message.requestVote.lastLogTerm();


        if (candidateTerm >= currentTerm
                && log.otherAsUpToDateAsThis(candidateLogLastTerm, candidateLogSize)
                && (votedFor == null || votedFor.equals(message.getSender())) ) {
            ControlMessage acceptance = new ControlMessage(ControlMessageType.REQUEST_VOTE_RESULT, true, message.sequenceNr);
            queueMessage(new RaftMessage(acceptance).setAckNr(message.sequenceNr), message.getSender());

            votedFor = message.getSender();
            setRole(ServerRole.FOLLOWER);
            lastLeaderContact = Instant.now();
            System.out.printf("[Vote] Server %d voted for %s in term %d.\n", id, message.getSender().getInetSocketAddress(), currentTerm);
        }
        else {
            ControlMessage rejection = new ControlMessage(ControlMessageType.REQUEST_VOTE_RESULT, false, message.sequenceNr);
            queueMessage(new RaftMessage(rejection).setAckNr(message.sequenceNr), message.getSender());

            System.out.printf("[Vote] Server %d rejected RV-RPC from %s in term %d: %s %s %s\n", id,
                    message.getSender().getInetSocketAddress(),
                    currentTerm,
                    candidateTerm >= currentTerm,
                    log.otherAsUpToDateAsThis(candidateLogLastTerm, candidateLogSize),
                    (votedFor == null || votedFor.equals(message.getSender())));
        }

        if (candidateTerm > currentTerm) currentTerm = candidateTerm;
    }

    private void handleControlMessage (RaftMessage message) {
        System.out.printf("[Control] Server %d received control message from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getControlMessage());
        switch (message.getControlMessage().type()) {
            case HELLO_SERVER -> {
                servers.add(message.getSender());
                clients.remove(message.getSender());
            }
            case HELLO_CLIENT -> {
                clients.add(message.getSender());
                servers.remove(message.getSender());
            }
            case REQUEST_VOTE_RESULT -> {
                if (message.getControlMessage().result()) {
                    currentElectionVotes++;
                    System.out.printf("[Election] Server %d received vote from %s.\n", id, message.getSender().getInetSocketAddress());
                }
                if (currentElectionVotes >= (clusterConfig.servers().size() + 1) / 2) {
                    System.out.printf("[Election] Server %d is now leader for term %d!\n", id, currentTerm);
                    setRole(ServerRole.LEADER);
                }
            }
            case null, default -> System.out.printf("RaftMessage with SEQ %d has unimplemented control type.\n", message.getSequenceNr());
        }
    }

    private void startElection() {

        setRole(ServerRole.CANDIDATE);
        currentTerm++;
        currentElectionVotes = 1; // Counts as voting for yourself
        votedFor = this;

        System.out.printf("[Election] Server %d starting election in term %d.\n", id, currentTerm);

        // Send a broadcast to all servers with RV-RPCs
        RequestVote rvRPC = new RequestVote(currentTerm, id, log.lastApplied, log.getLast().term());
        RaftMessage rvRPCBroadcast = new RaftMessage(rvRPC);
        rvRPCBroadcast.setTimeout(MSG_RETRY_INTERVAL);
        queueServerBroadcast(rvRPCBroadcast);
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

    private boolean checkElectionTimeout() {
        System.out.printf("[Election] Server %d timed out (role: %s).\n", this.id, role.toString());
        Duration timeSinceLeaderContact = Duration.between(lastLeaderContact, Instant.now());

        if (timeSinceLeaderContact.minus(electionTimeout).toMillis() >= 0 && role != ServerRole.LEADER) {
            lastLeaderContact = Instant.now();
            votedFor = null;
            currentElectionVotes = 0;
            return true;
        }
        return false;
    }

    public void setRole(ServerRole newRole) {
        if (role == newRole) return;

        if (role == ServerRole.LEADER) {
            heartbeatTimer.cancel();
        }
        if (newRole == ServerRole.LEADER) {
            scheduleHeartbeatMessages();
        }
        role = newRole;
        System.out.printf("[Role] Server %d switched to role %s.\n", id, role);
    }
}
