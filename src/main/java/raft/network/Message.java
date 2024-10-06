package raft.network;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public interface Message extends Serializable, Comparable<Message> {

    Node<?> getSender();

    void setSender(Node sender);

    String getMessage();

    void setMessage(String message);

    Instant deliveryTime();

    void setTimeout(Duration timeout);

    Instant timeoutInstant();

    MessageStatus getStatus();

    void setStatus(MessageStatus status);

    default Boolean timedOut() {
        return timeoutInstant().isBefore(Instant.now());
    }

    @Override
    default int compareTo(@NotNull Message that) {
        return timeoutInstant().compareTo(that.timeoutInstant());
    }
}
