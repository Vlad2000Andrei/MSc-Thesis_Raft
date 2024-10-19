package raft.classic;

import raft.benchmark.Crasher;
import raft.common.*;
import raft.messaging.common.ControlMessage;
import raft.messaging.common.ControlMessageType;
import raft.messaging.internal.AppendEntries;
import raft.messaging.common.RaftMessage;
import raft.messaging.internal.RequestVote;
import raft.network.Configuration;
import raft.network.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ClassicRaftServer extends RaftServer {

    public TimerTask heartbeatTimer;
    private int currentElectionVotes;

    // State from Paper
    private int currentTerm;
    private Node<RaftMessage> votedFor;
    private RaftLog log;
    protected ServerRole role;

    private HashMap<Integer, Integer> nextIndex;
    private HashMap<Integer, Integer> matchIndex;


    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
        log = new RaftLog();
        currentTerm = 0;

        role = ServerRole.FOLLOWER;
        electionTimeoutStartInstant = Instant.now();
        heartbeatTimer = null;
    }

    @Override
    public void runRaft() {
        Crasher crasher = new Crasher(0.0003, Duration.ofSeconds(3), Duration.ofSeconds(7));

        while(true) {
            try {
                // Check for election timeouts
                if (checkElectionTimeout()) {
                    startElection();
                }

                RaftMessage message = getNextMessage();
                if (message != null) {
                    handleMessage(message);
                }
                else {
                    System.out.println(".");
                }

                crasher.tryCrash(this);
            }
            catch (Exception e) {
                System.out.printf("[ERR] (%s)\t Error while running raft:\n", Thread.currentThread().getName());
                e.printStackTrace();
                System.exit(1);
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
        ControlMessage outcome;
        String reason = "";
        System.out.printf("[AppendEntries] Received: %s from %s.\n", message, message.getSender());

        // Reply false if they are from a past term
        if (message.appendEntries.term() < currentTerm) {
            outcome = new ControlMessage(ControlMessageType.APPEND_ENTRIES_RESULT, currentTerm, false, message.appendEntries.prevLogIdx() + 1);
            reason = String.format("Sender term %d behind current term %d.", message.appendEntries.term(), currentTerm);
        }
        else {
            // Update own term if sender has greater term
            if (message.appendEntries.term() > currentTerm) {
                setCurrentTerm(message.appendEntries.term());
                setRole(ServerRole.FOLLOWER);
            }

            // Try to insert the entries
            boolean inserted = tryStoreEntry(message.appendEntries);

            if (message.appendEntries.leaderCommit() > log.getCommittedIndex()) {
                int newCommittedIndex = Math.min(message.appendEntries.leaderCommit(), log.getSize());
                log.setCommittedIndex(newCommittedIndex);
            }

            outcome = new ControlMessage(ControlMessageType.APPEND_ENTRIES_RESULT, currentTerm, inserted, message.appendEntries.prevLogIdx() + message.appendEntries.entries().size());
            clearElectionTimeout();
        }

        if (outcome.result()) {
            System.out.printf(Colors.GREEN + "[AppendEntries] Server %d accepted %d new term %d entries from %s. Log size: %d (cIdx: %d).\n" + Colors.RESET, id, message.appendEntries.entries().size(), message.appendEntries.term(), message.getSender(), log.getSize(), log.getCommittedIndex());
        }
        else {
            System.out.printf(Colors.YELLOW + "[AppendEntries] Server %d rejected %d new entries from %s: %s\n" + Colors.RESET, id, message.appendEntries.entries().size(), message.getSender(), reason);
        }
        RaftMessage msg = new RaftMessage(outcome).setAckNr(message.sequenceNr);
        queueMessage(msg, message.getSender());
    }

    private void handleRequestVote (RaftMessage message){
        System.out.printf("[Vote] Server %d received RequestVote from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getRequestVote());

        int candidateTerm = message.requestVote.term();
        int candidateLogSize = message.requestVote.lastLogIndex();
        int candidateLogLastTerm = message.requestVote.lastLogTerm();

        // If the incoming RPC has a higher term, join that term and switch to FOLLOWER
        if (candidateTerm > currentTerm) {
            setCurrentTerm(candidateTerm);
            setRole(ServerRole.FOLLOWER);
            votedFor = null;
        }

        if (candidateTerm >= currentTerm
                && log.otherAsUpToDateAsThis(candidateLogLastTerm, candidateLogSize)
                && (votedFor == null || votedFor.equals(message.getSender())) ) {
            ControlMessage acceptance = new ControlMessage(ControlMessageType.REQUEST_VOTE_RESULT, currentTerm, true, message.sequenceNr);
            queueMessage(new RaftMessage(acceptance).setAckNr(message.sequenceNr), message.getSender());

            votedFor = message.getSender();
            setRole(ServerRole.FOLLOWER);
            clearElectionTimeout();
            System.out.printf("[Vote] Server %d voted for %s in term %d.\n", id, message.getSender().getInetSocketAddress(), currentTerm);
        }
        else {
            ControlMessage rejection = new ControlMessage(ControlMessageType.REQUEST_VOTE_RESULT, currentTerm, false, message.sequenceNr);
            queueMessage(new RaftMessage(rejection).setAckNr(message.sequenceNr), message.getSender());

            System.out.printf("[Vote] Server %d rejected RV-RPC %s from %s in term %d: %s %s %s\n", id,
                    message,
                    message.getSender().getInetSocketAddress(),
                    currentTerm,
                    candidateTerm >= currentTerm,
                    log.otherAsUpToDateAsThis(candidateLogLastTerm, candidateLogSize),
                    (votedFor == null || votedFor.equals(message.getSender())));
        }

    }

    private void handleControlMessage (RaftMessage message) {
//        System.out.printf("[Control] Server %d received control message from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getControlMessage());
        switch (message.getControlMessage().type()) {
            case REQUEST_VOTE_RESULT -> {

                if (message.getControlMessage().result() && message.controlMessage.term() == currentTerm) {
                    currentElectionVotes++;
//                    System.out.printf("[Election] Server %d received vote from %s in term %d.\n", id, message.getSender().getInetSocketAddress(), currentTerm);
                }

                if (role == ServerRole.CANDIDATE
                        && currentElectionVotes >= quorumSize()
                        && message.controlMessage.term() == currentTerm) {
                    System.out.printf(Colors.RED + "[Election] Server %d is now leader for term %d!\n" + Colors.RESET, id, currentTerm);
                    setRole(ServerRole.LEADER);
                }
                else if (message.controlMessage.term() > currentTerm) {
                    setCurrentTerm(message.controlMessage.term());
                    setRole(ServerRole.FOLLOWER);
                }
            }
            case APPEND_ENTRIES_RESULT -> {
                if (message.controlMessage.term() > currentTerm) {
                    setCurrentTerm(message.controlMessage.term());
                    setRole(ServerRole.FOLLOWER);
                }

                if (message.controlMessage.result()) {
                    long replicatedCount = handleAcceptedEntry((int)message.controlMessage.resultOf(), message.getSender().id);
                    if (replicatedCount >= quorumSize()) {
                        log.committedIndex.set((int)message.controlMessage.resultOf());
                    }
                }
                else {
                    handleRejectedEntry((int)message.controlMessage.resultOf(), message.getSender().id);
                }

            }
            case HELLO_SERVER -> {
                servers.add(message.getSender().setId((int)message.controlMessage.resultOf()));
                System.out.printf("[Infra] Server %d connected to %s. It now knows: \n\t- Servers: %s \n\t- Connections: %s\n", id, message.getSender(), servers, connections.values());
            }

            case null, default -> System.out.printf("RaftMessage with SEQ %d has unimplemented control type.\n", message.getSequenceNr());
        }
    }

    private void startElection() {

        setRole(ServerRole.CANDIDATE);
        setCurrentTerm(currentTerm + 1);
        currentElectionVotes = 1; // Counts as voting for yourself
        votedFor = this;

        System.out.printf("[Election] Server %d starting election in term %d. Needed votes: %d\n", id, currentTerm, quorumSize());

        // Send a broadcast to all servers with RV-RPCs
        RequestVote rvRPC = new RequestVote(currentTerm, id, log.committedIndex.get(), log.getLast().term());
        RaftMessage rvRPCBroadcast = new RaftMessage(rvRPC);//.setTimeout(MSG_RETRY_INTERVAL);
        queueServerBroadcast(rvRPCBroadcast);
    }

    public void scheduleHeartbeatMessages() {
        heartbeatTimer = new TimerTask() {
            @Override
            public void run() {
                queueServerBroadcast(new RaftMessage(
                        new AppendEntries(
                                currentTerm,
                                id,
                                log.getLastIndex(),
                                log.getLast().term(),
                                List.of(),
                                log.getCommittedIndex()
                        )));
            }
        };

        timeoutTimer.scheduleAtFixedRate(
               heartbeatTimer,
                HEARTBEAT_INTERVAL.toMillis() * 2,
                HEARTBEAT_INTERVAL.toMillis()
        );
    }

    public void descheduleHeartbeatMessage() {
        try {
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel();
                Thread.sleep(HEARTBEAT_INTERVAL.toMillis() * 2);
            }
        }
        catch (IllegalStateException ignored) {}
        catch (InterruptedException ignored) {}
        finally {
            heartbeatTimer = null;
        }
    }

    private boolean checkElectionTimeout() {
        Duration timeSinceLeaderContact = Duration.between(electionTimeoutStartInstant, Instant.now());

        if (timeSinceLeaderContact.minus(electionTimeout).toMillis() >= 0 && role != ServerRole.LEADER) {
            System.out.printf(Colors.PURPLE + "[Election] Server %d timed out at %s (role: %s, Messages in queue: %d).\n" + Colors.RESET, this.id, Instant.now(), role.toString(), incomingMessages.size());
            clearElectionTimeout();
            currentElectionVotes = 0;
            newRandomTimeout();
            return true;
        }
        electionTimeoutStartInstant = Instant.now();
        return false;
    }

    public void setRole(ServerRole newRole) {
        if (role == newRole) return;

        if (role == ServerRole.LEADER) {
            descheduleHeartbeatMessage();
        }
        if (newRole == ServerRole.LEADER) {
            scheduleHeartbeatMessages();
            matchIndex = new HashMap<>();
            nextIndex = new HashMap<>();

            servers.forEach(server -> {
                matchIndex.put(server.id, 0);
                nextIndex.put(server.id, log.getLastIndex() + 1);
            });

            createEntry();
        }
        role = newRole;
        System.out.printf(Colors.RED + "[Role] Server %d switched to role %s.\n" + Colors.RESET, id, role);
    }

    public ServerRole getRole() {
        return this.role;
    }

    private void newRandomTimeout() {
        electionTimeout = Duration.ofMillis(new Random().nextInt((int)ELECTION_TIMEOUT_MIN.toMillis(), (int)ELECTION_TIMEOUT_MAX.toMillis()));
        System.out.printf("[Election] Server %d's new election timeout is %dms.\n", id, electionTimeout.toMillis());
    }

    public void clearElectionTimeout() {
        electionTimeoutStartInstant = Instant.now();
//        System.out.printf(Colors.PURPLE + "[Election] Server %d reset election timeout of %dms at %s.\n" + Colors.RESET, id, electionTimeout.toMillis(), Instant.now());
    }

    private void setCurrentTerm (int newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            System.out.printf(Colors.RED + "[Raft] Server %d moved to term %d.\n" + Colors.RESET, id, currentTerm);
        }
        else {
            throw new RuntimeException(String.format("Trying to switch turn to %d from %d", newTerm, currentTerm));
        }
    }

    private long handleAcceptedEntry(int index, int serverId) {
        matchIndex.put(serverId, index);
        nextIndex.put(serverId, index + 1);

        updateFollowerLogs();

        return matchIndex.values()
                .stream()
                .filter(idx -> idx >= index)
                .count();
    }

    private void handleRejectedEntry(int index, int serverId) {
        nextIndex.compute(serverId, (id,idx) -> Math.min(0, index - 1));
        RaftMessage updatedAERPC = createAppendEntryMessage(nextIndex.get(serverId));

        Node<RaftMessage> server = getFirstServerById(serverId);
        queueMessage(updatedAERPC, server);

        updateFollowerLogs();
    }

    private Node<RaftMessage> getFirstServerById (int serverId) {
        return servers.stream()
                .filter(node -> node.id != null && node.id == serverId) // node.id == null if we run this before a server has connected
                .toList()
                .getFirst();
    }

    private void createEntry() {
        // Create new entry
        LogEntry entry = new LogEntry(currentTerm);
        log.add(entry);


        // Announce to all servers
        RaftMessage aeRPC = createAppendEntryMessage(log.getLastIndex());
        queueServerBroadcast(aeRPC);
    }

    private boolean tryStoreEntry (AppendEntries msg) {
        if (log.hasMatchingEntry(msg.prevLogIdx(), msg.prevLogTerm())) {
            LogEntry entryToInsert = null;
            if (!msg.entries().isEmpty()) entryToInsert = msg.entries().getFirst();

            log.insertEntry(msg.prevLogIdx() + 1, entryToInsert);
            return true;
        }
        return false;
    }

    private RaftMessage createAppendEntryMessage(int idx) {
        if (idx == 0) idx = 1;
        int previousEntryIdx = idx -1;
        LogEntry previousEntry = log.get(previousEntryIdx);

        AppendEntries appendEntries = new AppendEntries(currentTerm,
                id,
                previousEntryIdx,
                previousEntry.term(),
                List.of(log.get(idx)),
                log.getCommittedIndex());
        return new RaftMessage(appendEntries);//.setTimeout(HEARTBEAT_INTERVAL);
    }

    private void updateFollowerLogs() {
        if (role != ServerRole.LEADER) return;

        nextIndex.entrySet()
                .stream()
                .filter(entry -> entry.getValue() < log.getLastIndex() + 1 && !entry.getKey().equals(id)) // servers that are not this server and have a lower nextIndex
                .forEach(entry -> {
                    try {
                        Node<RaftMessage> server = getFirstServerById(entry.getKey());
                        RaftMessage msg = createAppendEntryMessage(entry.getValue());
                        queueMessage(msg, server);
                    }
                    catch (NoSuchElementException e) {
                        System.out.printf(Colors.YELLOW + "[Timing] Skipping updates for Server %d because it cannot be found.\n" + Colors.RESET, entry.getKey());
                    }
                });
    }

    public static void main (String[] args) {
        try {
            int ownId = Integer.parseInt(args[0]);
            String configFilePath = args[1];

            File configFile = new File(configFilePath);
            FileInputStream fis = new FileInputStream(configFile);
            Scanner fileScanner = new Scanner(fis);

            List<Node<RaftMessage>> peers = new ArrayList<>();
            RaftServer thisServer = null;

            while (fileScanner.hasNextLine()) {
                String line = fileScanner.nextLine();
                String[] peerDetails = line.split(" ");
                System.out.println(Arrays.toString(peerDetails));

                int peerId = Integer.parseInt(peerDetails[0]);
                String peerAddress = peerDetails[1];
                int peerPort = Integer.parseInt(peerDetails[2]);

                if (peerId == ownId) {
                    thisServer = new ClassicRaftServer(new InetSocketAddress(peerAddress, peerPort));
                    thisServer.id = ownId;
                    peers.add(thisServer);
                }
                else {
                    peers.add(new Node<RaftMessage>(new InetSocketAddress(peerAddress, peerPort)).setId(peerId));
                }
            }
            fis.close();

            Configuration cluster = new Configuration(peers);
            thisServer.start(cluster);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
