package raft.common;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

public record LogEntry(
        int term,
        int leaderId,
        Instant creationTime,
        Instant storageTime
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 555555L;
}
