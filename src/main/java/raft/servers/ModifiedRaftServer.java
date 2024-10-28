package raft.servers;

import raft.common.Colors;
import raft.common.RaftServer;
import raft.common.ServerRole;
import raft.messaging.common.RaftMessage;
import raft.messaging.internal.RequestVote;
import raft.messaging.internal.RequestVoteResponse;
import raft.messaging.internal.VoteResponseStatus;
import raft.network.Configuration;
import raft.network.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ModifiedRaftServer extends ClassicRaftServer {

    Node<RaftMessage> nextVote;
    RaftMessage nextVoteMsg;

    public ModifiedRaftServer(InetSocketAddress address) throws IOException {
        super(address);
    }

    @Override
    protected void handleRequestVote(RaftMessage message) {
        System.out.printf("[Vote] Server %d received RequestVote from %s: %s.\n", id, message.getSender().getInetSocketAddress(), message.getRequestVote());

        int candidateTerm = message.requestVote.term();
        int candidateLogSize = message.requestVote.lastLogIndex();
        int candidateLogLastTerm = message.requestVote.lastLogTerm();

        // If the incoming RPC has a higher term, join that term and switch to FOLLOWER
        if (candidateTerm > currentTerm) {
            setCurrentTerm(candidateTerm);
            setRole(ServerRole.FOLLOWER);
            votedFor = null;
            nextVoteMsg = null;
            nextVote = null;
            currentElectionVotes = 0;
        }

        if (candidateTerm == currentTerm
                && log.otherAsUpToDateAsThis(candidateLogLastTerm, candidateLogSize)) {
            // If the candidate has a lower ID, refuse and join the race
            if (message.getSender().id < id && log.asUpToDateAsOther(candidateLogLastTerm, candidateLogSize)) {
                RequestVoteResponse rejection = new RequestVoteResponse(VoteResponseStatus.REJECT, currentTerm);
                queueMessage(new RaftMessage(rejection).setAckNr(message.sequenceNr), message.getSender());
                clearElectionTimeout();
                startElection();
            }
            else {
                // easy path: haven't voted yet
                if (votedFor == null || votedFor.id == this.id) {
                    votedFor = message.getSender();
                    setRole(ServerRole.FOLLOWER);
                    RequestVoteResponse accept = new RequestVoteResponse(VoteResponseStatus.ACCEPT, currentTerm);
                    queueMessage(new RaftMessage(accept).setAckNr(message.sequenceNr), message.getSender());
                    clearElectionTimeout();
                }
                // complex path: retract our previous vote
                else {
                    RequestVoteResponse retraction = new RequestVoteResponse(VoteResponseStatus.CANCEL, currentTerm);
                    queueMessage(new RaftMessage(retraction), votedFor);    // cancel our previous vote

                    // Prepare our acceptance message, but don't send it
                    // It will be sent once `handleRequestVoteResponse` sees an `ACK_CANCEL` response from `votedFor`.
                    RequestVoteResponse accept = new RequestVoteResponse(VoteResponseStatus.ACCEPT, currentTerm);
                    nextVoteMsg = new RaftMessage(accept).setAckNr(message.getSequenceNr());
                    nextVote = message.getSender(); // remember that we need to vote for this node
                }
            }
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

    @Override
    protected void handleRequestVoteResponse(RaftMessage message) {
        int messageTerm = message.getRequestVoteResponse().term();
        if (messageTerm > currentTerm) {
            setCurrentTerm(messageTerm);
            setRole(ServerRole.FOLLOWER);
        }
        else if (messageTerm == currentTerm) {
            switch (message.getRequestVoteResponse().result()) {
                case ACCEPT -> {
                    // if the server is still in the race, count the vote
                    if (role == ServerRole.CANDIDATE) currentElectionVotes++;
                }
                case CANCEL -> {
                    // Someone somewhere found a node with a higher ID, so this node backs out.
                    setRole(ServerRole.FOLLOWER);
                    currentElectionVotes = 0;
                    votedFor = null;
                    nextVote = null;
                    nextVoteMsg = null;
                    RequestVoteResponse ackCancel = new RequestVoteResponse(VoteResponseStatus.ACK_CANCEL, currentTerm);
                    queueMessage(new RaftMessage(ackCancel).setAckNr(message.getSequenceNr()), message.getSender());
                    clearElectionTimeout();
                }
                case ACK_CANCEL -> {
                    // Our CANCEL was ACK'ed, so now we send the accept to the waiting candidate
                    if (nextVote != null && nextVoteMsg != null) { // sanity check
                        nextVoteMsg.setRequestVoteResponse(new RequestVoteResponse(VoteResponseStatus.ACCEPT, currentTerm));
                        queueMessage(nextVoteMsg, nextVote);
                        votedFor = nextVote;
                        nextVote = null;
                        nextVoteMsg = null;
                        setRole(ServerRole.FOLLOWER);
                        currentElectionVotes = 0;
                    }
                }
                case REJECT -> {
                    break;
                }
            }
        }

        if (currentElectionVotes >= quorumSize()) {
            setRole(ServerRole.LEADER);
            System.out.printf(Colors.RED + "[Election] Server %d is now leader for term %d!\n" + Colors.RESET, id, currentTerm);
        }
    }

    @Override
    protected void startElection() {
        super.startElection();
        nextVote = null;
        nextVoteMsg = null;
    }

    public static void main (String[] args) {
        try {
            int ownId = Integer.parseInt(args[0]);
            String configFilePath = args[1];

            System.out.printf("[ModifiedRaftServer] Opening config file %s\n", configFilePath);
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
                    thisServer = new ModifiedRaftServer(new InetSocketAddress(peerAddress, peerPort));
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
