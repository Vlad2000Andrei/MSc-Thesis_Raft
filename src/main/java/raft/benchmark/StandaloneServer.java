package raft.benchmark;

import raft.classic.EchoRaftServer;
import raft.common.RaftServer;

import java.net.InetSocketAddress;

public class StandaloneServer {

    public static void main (String[] args) {
        int startPort = 55000;

        try {
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", startPort);
            RaftServer server = new EchoRaftServer(serverAddress);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
