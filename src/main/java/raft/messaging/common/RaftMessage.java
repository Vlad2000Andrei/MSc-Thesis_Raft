package raft.messaging.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import raft.messaging.internal.AppendEntries;
import raft.messaging.internal.RequestVote;
import raft.network.Node;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class RaftMessage implements Message, Serializable {

    @JsonIgnore
    Node<RaftMessage> sender;
    @JsonIgnore
    Node<RaftMessage> receiver;
    @JsonIgnore
    Duration timeout;
    public long sequenceNr;
    public long ackNr;
    @JsonIgnore
    private static AtomicInteger nextSequenceNr = new AtomicInteger(0);
    public ControlMessage controlMessage;
    public AppendEntries appendEntries;
    public RequestVote requestVote;

    public RaftMessage(@Nullable ControlMessage controlMessage, @Nullable AppendEntries appendEntries, @Nullable RequestVote requestVote, long ackNr, long sequenceNr) {
        if (sequenceNr == -1) {
            this.sequenceNr = nextSequenceNr.getAndIncrement();
        }
        long numAssigned = Stream.of(controlMessage, appendEntries, requestVote).filter(Objects::nonNull).count();

        if (numAssigned > 1) {
            throw new RuntimeException("Assigning more than one message type is not allowed.");
        }
        this.controlMessage = controlMessage;
        this.appendEntries = appendEntries;
        this.requestVote = requestVote;
        this.ackNr = ackNr;

        timeout = null;
    }

    public RaftMessage(@NotNull RaftMessage toCopy) {
        this(toCopy.controlMessage,
                toCopy.appendEntries,
                toCopy.requestVote,
                toCopy.ackNr,
                -1);
        setReceiver(toCopy.getReceiver());
        setSender(toCopy.getSender());
        setTimeout(toCopy.getTimeout());
    }

    public RaftMessage(@Nullable ControlMessage ctrl, @Nullable AppendEntries append, @Nullable RequestVote reqVote) {
        this(ctrl, append, reqVote, -1, -1);
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

    public RaftMessage(long ackNr) {
        this(null, null, null, ackNr, -1);
    }

    public RaftMessage() {
        this(null, null, null);
    }

    @JsonIgnore
    @Override
    public Node<RaftMessage> getSender() {
        return sender;
    }

    @JsonIgnore
    @Override
    public void setSender(Node<RaftMessage> sender) {
        this.sender = sender;
    }

    @JsonIgnore
    @Override
    public Node<RaftMessage> getReceiver() {
        return receiver;
    }

    @JsonIgnore
    @Override
    public Duration getTimeout() {
        return timeout;
    }

    @JsonIgnore
    @Override
    public RaftMessage setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    @JsonIgnore
    @Override
    public RaftMessage setReceiver(Node<RaftMessage> receiver) {
        this.receiver = receiver;
        return this;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }

    public AppendEntries getAppendEntries() {
        return appendEntries;
    }

    public void setAppendEntries(AppendEntries appendEntries) {
        this.appendEntries = appendEntries;
    }

    public RequestVote getRequestVote() {
        return requestVote;
    }
    public void setRequestVote(RequestVote requestVote) {
        this.requestVote = requestVote;
    }

    public long getSequenceNr() {
        return sequenceNr;
    }

    public RaftMessage setSequenceNr(long sequenceNr) {
        this.sequenceNr = sequenceNr;
        return this;
    }

    public long getAckNr() {
        return ackNr;
    }

    public RaftMessage setAckNr(long ackNr) {
        this.ackNr = ackNr;
        return this;
    }

    @Override
    public String toString() {
        return String.format("SEQ: %d \tACK: %d \tCTRL: %s \tRV: %s \tAE: %s",
                sequenceNr,
                ackNr,
                controlMessage,
                requestVote,
                appendEntries);
    }
}
