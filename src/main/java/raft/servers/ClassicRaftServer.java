package raft.servers;

import raft.benchmark.Crasher;
import raft.common.*;
import raft.messaging.internal.*;
import raft.messaging.common.RaftMessage;
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
    protected int currentElectionVotes;

    // State from Paper
    protected int currentTerm;
    protected Node<RaftMessage> votedFor;
    private HashMap<Integer, Integer> nextIndex;
    private HashMap<Integer, Integer> matchIndex;


    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
        currentTerm = 0;
        electionTimeoutStartInstant = Instant.now();
        heartbeatTimer = null;
    }

    @Override
    public void runRaft() {
        Crasher crasher = new Crasher(0.0005, Duration.ofSeconds(5), Duration.ofSeconds(7), Duration.ofSeconds(15));
        Instant lastSimulatedEntryAt = Instant.now();
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

                // Pretent to get a client request
                if (role == ServerRole.LEADER) {
                    if (Duration.between(lastSimulatedEntryAt, Instant.now()).toMillis() > 20) {
                        createEntry();
                        lastSimulatedEntryAt = Instant.now();
                    }
                }

                crasher.tryCrash(this, true);
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
        else if (message.getRequestVoteResponse() != null) {
            handleRequestVoteResponse(message);
        }
        else if (message.getAppendEntriesResponse() != null) {
            handleAppendEntriesResponse(message);
        }
        else {
            throw new NoSuchMethodException("Message has no content, cannot find matching handler.");
        }
    }

    protected void handleAppendEntries (RaftMessage message){
        AppendEntriesResponse outcome;
        String reason = "";
//        System.out.printf("[AppendEntries] Received: %s from %s.\n", message.appendEntries, message.getSender());

        // Reply false if they are from a past term
        if (message.appendEntries.term() < currentTerm) {
            outcome = new AppendEntriesResponse(currentTerm, false, message.appendEntries.prevLogIdx() + 1);
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
                int newCommittedIndex = Math.min(message.appendEntries.leaderCommit(), log.getLastIndex());
                log.setCommittedIndex(newCommittedIndex);
            }

            outcome = new AppendEntriesResponse(currentTerm, inserted, message.appendEntries.prevLogIdx() + message.appendEntries.entries().size());
            clearElectionTimeout();
        }

//        if (outcome.result()) {
//            System.out.printf(Colors.GREEN + "[AppendEntries] Server %d accepted %d new term %d entries from %s. Last idx: %d (cIdx: %d, qSize: %d).\n" + Colors.RESET, id, message.appendEntries.entries().size(), message.appendEntries.term(), message.getSender(), log.getLastIndex(), log.getCommittedIndex(), incomingMessages.size());
//        }
//        else {
//            System.out.printf(Colors.YELLOW + "[AppendEntries] Server %d rejected %d new entries from %s: %s. (cIdx: %d, qSize: %d)\n" + Colors.RESET, id, message.appendEntries.entries().size(), message.getSender(), reason, log.getCommittedIndex(), incomingMessages.size());
//        }
        RaftMessage msg = new RaftMessage(outcome).setAckNr(message.sequenceNr);
        queueMessage(msg, message.getSender());
    }

    protected void handleAppendEntriesResponse (RaftMessage message) {
        if (role != ServerRole.LEADER) return;  // if it's an old message from when we were leader, ignore it
        int messageTerm = message.getAppendEntriesResponse().term();
        boolean messageResult = message.getAppendEntriesResponse().result();
        int messageEntryIdx = message.appendEntriesResponse.entryIndex();

        if (messageTerm > currentTerm) {
            setCurrentTerm(messageTerm);
            setRole(ServerRole.FOLLOWER);
        }
        if (messageResult) {
            long replicatedCount = handleAcceptedEntry(messageEntryIdx, message.getSender().id);
            if (replicatedCount >= quorumSize()) {
                log.setCommittedIndex(Math.max(messageEntryIdx, log.getCommittedIndex()));
            }
        }
        else {
            handleRejectedEntry(messageEntryIdx, message.getSender().id);
        }
    }

    protected void handleRequestVote (RaftMessage message){
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
            RequestVoteResponse acceptance = new RequestVoteResponse(VoteResponseStatus.ACCEPT, currentTerm);
            queueMessage(new RaftMessage(acceptance).setAckNr(message.sequenceNr), message.getSender());

            votedFor = message.getSender();
            setRole(ServerRole.FOLLOWER);
            clearElectionTimeout();
            System.out.printf("[Vote] Server %d voted for %s in term %d.\n", id, message.getSender().getInetSocketAddress(), currentTerm);
        }
        else {
            RequestVoteResponse rejection = new RequestVoteResponse(VoteResponseStatus.REJECT, currentTerm);
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

    protected void handleRequestVoteResponse(RaftMessage message) {
        int messageTerm = message.getRequestVoteResponse().term();
        boolean messageResult = switch (message.getRequestVoteResponse().result()) {
            case ACCEPT -> true;
            case REJECT, CANCEL, ACK_CANCEL -> false;
        };

        if (messageResult && messageTerm == currentTerm) {
            currentElectionVotes++;
        }

        if (role == ServerRole.CANDIDATE
                && currentElectionVotes >= quorumSize()
                && messageTerm == currentTerm) {
            System.out.printf(Colors.RED + "[Election] Server %d is now leader for term %d!\n" + Colors.RESET, id, currentTerm);
            setRole(ServerRole.LEADER);
        }
        else if (messageTerm > currentTerm) {
            setCurrentTerm(messageTerm);
            setRole(ServerRole.FOLLOWER);
        }
    }

    protected void handleControlMessage (RaftMessage message) {
//        System.out.printf("[Control] Server %d received control message from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getControlMessage());
        switch (message.getControlMessage().type()) {
            case HELLO_SERVER -> {
                servers.add(message.getSender().setId((int)message.controlMessage.resultOf()));
                System.out.printf("[Infra] Server %d connected to %s. It now knows: \n\t- Servers: %s \n\t- Connections: %s\n", id, message.getSender(), servers, connections.values());
            }

            default -> System.out.printf("RaftMessage with SEQ %d has unimplemented control type.\n", message.getSequenceNr());
        }
    }

    protected void startElection() {
        setRole(ServerRole.CANDIDATE);
        setCurrentTerm(currentTerm + 1);
        currentElectionVotes = 1; // Counts as voting for yourself
        votedFor = this;

        System.out.printf("[Election] Server %d starting election in term %d. Needed votes: %d\n", id, currentTerm, quorumSize());

        // Send a broadcast to all servers with RV-RPCs
        RequestVote rvRPC = new RequestVote(currentTerm, id, log.getLastIndex(), log.getLast().term());
        RaftMessage rvRPCBroadcast = new RaftMessage(rvRPC);//.setTimeout(MSG_RETRY_INTERVAL);
        queueServerBroadcast(rvRPCBroadcast);
    }

    public void scheduleHeartbeatMessages() {
        System.out.printf(Colors.RED + "[Heartbeat] Scheduling heartbeat messages every %dms starting at %s.\n" + Colors.RESET, HEARTBEAT_INTERVAL.toMillis(), Instant.now());
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

    protected void setCurrentTerm(int newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            clearElectionTimeout();
            newRandomTimeout();
            System.out.printf(Colors.RED + "[Raft] Server %d moved to term %d.\n" + Colors.RESET, id, currentTerm);
        }
        else {
            throw new RuntimeException(String.format("Trying to switch turn to %d from %d", newTerm, currentTerm));
        }
    }

    private long handleAcceptedEntry(int index, int serverId) {
        // Count the acceptance
        matchIndex.put(serverId, index);
        nextIndex.put(serverId, Math.max(index + 1, nextIndex.get(serverId)));

        // If there are more outstanding entries, send next one
        if (nextIndex.get(serverId) <= log.getLastIndex()) {
            RaftMessage nextEntry = createAppendEntryMessage(nextIndex.get(serverId));
            queueMessage(nextEntry, getFirstServerById(serverId));
        }

        return matchIndex.values()
                .stream()
                .filter(idx -> idx >= index)
                .count();
    }

    private void handleRejectedEntry(int index, int serverId) {
        nextIndex.compute(serverId, (id,idx) -> Math.max(0, index - 1));

        // if this is an old request, the server might have accepted this in the meantime.
        // this is a non-official optimization
        if (matchIndex.get(serverId) >= nextIndex.get(serverId)) {
            nextIndex.put(serverId, nextIndex.get(serverId));
            return;
        }

        RaftMessage updatedAERPC = createAppendEntryMessage(nextIndex.get(serverId));
        Node<RaftMessage> server = getFirstServerById(serverId);
        queueMessage(updatedAERPC, server);
    }

    private Node<RaftMessage> getFirstServerById (int serverId) {
        return servers.stream()
                .filter(node -> node.id != null && node.id == serverId) // node.id == null if we run this before a server has connected
                .toList()
                .get(0);
    }

    private void createEntry() {
        // Create new entry
        LogEntry entry = new LogEntry(currentTerm, id, Instant.now(), Instant.now());
        log.add(entry);
        matchIndex.put(id, log.getLastIndex()); // Count self for committing purposes

        // Announce to all servers
        updateFollowerLogs();
    }

    private boolean tryStoreEntry (AppendEntries msg) {
        if (log.hasMatchingEntry(msg.prevLogIdx(), msg.prevLogTerm())) {
            LogEntry entryToInsert = null;
            if (!msg.entries().isEmpty()) entryToInsert = msg.entries().get(0);

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
                .filter(entry -> entry.getValue() <= log.getLastIndex() && !entry.getKey().equals(id)) // servers that are not this server and have a lower nextIndex
                .forEach(entry -> {
                    try {
                        Node<RaftMessage> server = getFirstServerById(entry.getKey());
                        RaftMessage msg = createAppendEntryMessage(entry.getValue());
                        queueMessage(msg, server);
                    }
                    catch (NoSuchElementException ignored) {}
                });
    }

    public static void main (String[] args) {
        try {
            int ownId = Integer.parseInt(args[0]);
            String configFilePath = args[1];

            System.out.printf("[ClassicRaftServer] Opening config file %s\n", configFilePath);
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
