package raft.benchmark;

import raft.common.LogEntry;
import raft.network.Configuration;

import java.io.Serializable;
import java.util.List;

public record BenchmarkControlMessage (
        BenchmarkControlMessageType type,
        List<LogEntry> logEntries,
        Integer serverId
) implements Serializable { }
