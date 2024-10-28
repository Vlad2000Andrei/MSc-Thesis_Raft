package raft.benchmark;

import java.io.Serializable;

public enum BenchmarkControlMessageType implements Serializable {
    LOG,
    LOG_OK,
    STOP,
    STOP_OK,
    START_CLASSIC,
    START_MODIFIED,
    HELLO
}
