package raft.messaging.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.jetbrains.annotations.Nullable;
import raft.messaging.internal.AppendEntries;
import raft.messaging.internal.RequestVote;
import raft.network.Node;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

public class RaftMessage implements Message, Serializable {

    @JsonIgnore
    Node<RaftMessage> sender;
    @JsonIgnore
    Node<RaftMessage> receiver;
    @JsonIgnore
    Duration timeout;
    private final ControlMessage controlMessage;
    private final AppendEntries appendEntries;
    private final RequestVote requestVote;

    private RaftMessage(@Nullable ControlMessage ctrl, @Nullable AppendEntries append, @Nullable RequestVote reqVote) {
        long numAssigned = Stream.of(ctrl, append, reqVote).filter(Objects::nonNull).count();

        if (numAssigned > 1) {
            throw new RuntimeException("Assigning more than one message type is not allowed.");
        }

        controlMessage = ctrl;
        appendEntries = append;
        requestVote = reqVote;

        timeout = null;
    }

    public RaftMessage(ControlMessage controlMessage) {
        this(controlMessage, null, null);
    }

    public RaftMessage(AppendEntries appendEntries) {
        this(null, appendEntries, null);
    }

    public RaftMessage(RequestVote requestVote) {
        this(null, null, requestVote);
    }

    @Override
    public Node<RaftMessage> getSender() {
        return sender;
    }

    @Override
    public void setSender(Node<RaftMessage> sender) {
        this.sender = sender;
    }

    @Override
    public Node<RaftMessage> getReceiver() {
        return receiver;
    }

    @Override
    public Duration getTimeout() {
        return timeout;
    }

    @Override
    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    @Override
    public void setReceiver(Node<RaftMessage> receiver) {
        this.receiver = receiver;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public AppendEntries getAppendEntries() {
        return appendEntries;
    }

    public RequestVote getRequestVote() {
        return requestVote;
    }
}
