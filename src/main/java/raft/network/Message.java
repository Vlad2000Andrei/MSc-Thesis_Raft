package raft.network;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;

public interface Message extends Serializable, Comparable<Message> {

    Node<?> getSender();

    Instant getDeliveryTime();

    void setTimeout(Duration timeout);

    Instant getTimeoutInstant();

    MessageStatus getStatus();

    void setStatus(MessageStatus status);

    default Boolean timedOut() {
        return getTimeoutInstant().isBefore(Instant.now());
    }

    @Override
    default int compareTo(@NotNull Message that) {
        return getTimeoutInstant().compareTo(that.getTimeoutInstant());
    }
}
