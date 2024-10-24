package raft.common;

import java.io.Serializable;
import java.time.Instant;

public record LogEntry(
        int term,
        int leaderId,
        Instant creationTime
) implements Serializable {}
