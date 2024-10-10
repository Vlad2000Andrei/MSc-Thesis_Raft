package raft.benchmark;

import raft.classic.EchoRaftServer;
import raft.common.RaftServer;
import raft.network.Configuration;

import java.net.InetSocketAddress;
import java.util.List;

public class StandaloneServer {

    public static void main (String[] args) {
        int startPort = 55000;

        try {
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", startPort);
            RaftServer server = new EchoRaftServer(serverAddress);
            server.id = 0;
            server.start(new Configuration(List.of(server)));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
