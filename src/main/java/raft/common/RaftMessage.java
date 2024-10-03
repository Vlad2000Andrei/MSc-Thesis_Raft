package raft.common;

import raft.network.Message;
import raft.network.Node;

import java.io.Serializable;

public abstract class RaftMessage implements Message, Serializable {

    @Override
    public Node<RaftMessage> getSender() {
        return null;
    }
}
