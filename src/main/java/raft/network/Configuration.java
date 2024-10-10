package raft.network;

import raft.messaging.common.RaftMessage;

import java.util.List;

public record Configuration(
        List<Node<RaftMessage>> servers
) {
}
