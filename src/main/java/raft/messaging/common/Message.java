package raft.messaging.common;

import org.jetbrains.annotations.NotNull;
import raft.network.Node;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public interface Message extends Serializable {

    Node<RaftMessage> getSender();

    void setSender(Node<RaftMessage> sender);

    Node<RaftMessage> getReceiver();

    Message setReceiver(Node<RaftMessage> receiver);

    Duration getTimeout();

    Message setTimeout(Duration timeout);

}
