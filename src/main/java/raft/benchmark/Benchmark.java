package raft.benchmark;

import raft.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Node;
import raft.network.SocketConnection;

import java.net.InetSocketAddress;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        System.out.println("Hello world!");

        try {
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", 55000);
            RaftServer server = new RaftServer(serverAddress);
            server.start();

            InetSocketAddress clientAddress = new InetSocketAddress("localhost", 55001);
            Node<RaftMessage> client = new Node<>(clientAddress);
            SocketConnection<RaftMessage> toServer = client.connectTo(server);

            toServer.send(new RaftMessage("Hi"));
            System.out.println(server.getNextMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
