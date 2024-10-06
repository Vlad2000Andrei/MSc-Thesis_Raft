package raft.classic;

import raft.common.RaftMessage;
import raft.common.RaftServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class EchoRaftServer extends RaftServer {


    public EchoRaftServer(InetSocketAddress address) throws IOException {
        super(address);
    }

    @Override
    public void runRaft() {
        while (true) {
            RaftMessage msg = getNextMessage();
            queueMessage(msg, msg.getSender());
        }
    }
}
