package raft.network;

import java.io.Serializable;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.Instant;

public interface Message extends Serializable {

    Node<?> getSender();

    Instant getDeliveryTime();

    void setTimeout(Duration timeout);

    Instant getTimeoutInstant();

    default Boolean timedOut() {
        return getTimeoutInstant().isBefore(Instant.now());
    }

}
