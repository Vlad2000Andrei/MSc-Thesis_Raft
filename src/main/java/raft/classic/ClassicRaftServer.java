package raft.classic;

import raft.common.RaftServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ClassicRaftServer extends RaftServer {
    public ClassicRaftServer(InetSocketAddress address) throws IOException {
        super(address);
    }

    @Override
    public void runRaft() {

    }


}
