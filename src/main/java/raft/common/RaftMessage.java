package raft.common;

import raft.network.Message;
import raft.network.MessageStatus;
import raft.network.Node;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class RaftMessage implements Message, Serializable {

    public String content;

    MessageStatus status;

    Duration timeoutDuration;

    public RaftMessage(String msg) {
        content = msg;
    }

    @Override
    public Instant getDeliveryTime() {
        return Instant.now();
    }

    @Override
    public void setTimeout(Duration timeout) {
        timeoutDuration = timeout;
    }

    @Override
    public Instant getTimeoutInstant() {
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
        return null;
    }
}
