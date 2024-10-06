package raft.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import raft.network.Message;
import raft.network.MessageStatus;
import raft.network.Node;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class RaftMessage implements Message, Serializable {

    @JsonProperty("message")
    public String message;
    @JsonProperty("status")
    MessageStatus status;
    @JsonProperty("timeout")
    Duration timeoutDuration;

    @JsonIgnore
    Node<RaftMessage> sender;

    public RaftMessage() {
        this("", MessageStatus.READY, Duration.ZERO);
    }

    public RaftMessage(String msg) {
        this(msg, MessageStatus.READY, Duration.ZERO);
    }

    public RaftMessage(String msg, MessageStatus status, Duration timeoutDuration) {
        this.message = msg;
        this.status = status;
        this.timeoutDuration = timeoutDuration;
    }

    @Override
    public Instant deliveryTime() {
        return Instant.now();
    }

    @Override
    public void setTimeout(Duration timeout) {
        timeoutDuration = timeout;
    }

    @Override
    public Instant timeoutInstant() {
        return Instant.now().plus(timeoutDuration);
    }

    @Override
    public MessageStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(MessageStatus status) {
        this.status = status;
    }

    @Override
    public Node<RaftMessage> getSender() {
        return sender;
    }

    @Override
    public void setSender(Node sender) {
        this.sender = sender;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("|| [RaftMessage] Content: %s\t  Status: %s\t  Timeout: %s ||\t",
                message, status.toString(), timeoutInstant().toString());
    }
}
