package raft.benchmark;

import raft.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Node;
import raft.network.SocketConnection;

import java.net.InetSocketAddress;
import java.util.Objects;

public class StressTestWorker implements Runnable {

    private Node<RaftMessage> server;
    private int opCount;
    private int port;

    public StressTestWorker(Node<RaftMessage> server, int opCount, int port) {
        this.server = server;
        this.opCount = opCount;
        this.port = port;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + " starting stress test...");
        try {
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", port);
            Node<RaftMessage> client = new Node<>(clientAddress);
            SocketConnection toServer = client.connectTo(server);
            System.out.printf("%s connected to server (%s -> %s)\n",
                    Thread.currentThread().getName(),
                    toServer.getNonBlockingChannel().socket().getLocalPort(),
                    toServer.getNonBlockingChannel().socket().getPort());


            int progressStepSize = opCount / 10;
            for (int i = 0; i < opCount; i++) {
                String content = "Message #" + i;
                toServer.send(new RaftMessage(content));
                if (!toServer.receive().message.equals(content)) {
                    throw new RuntimeException("Received message does not match sent message!");
                }
                if (i % progressStepSize == 0 && i > 0) System.out.printf("%s:\t %2.2f%% done.\n", Thread.currentThread().getName(), i / (double)progressStepSize * 10);
            }
            toServer.close();
            System.out.println(Thread.currentThread().getName() + " finished stress test!");
        }
        catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + " failed stress test!");
            throw new RuntimeException(e);
        }
    }
}
