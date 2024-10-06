package raft.benchmark;

import raft.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Node;
import raft.network.SocketConnection;

import java.net.InetSocketAddress;

public class StressTestWorker implements Runnable {

    private RaftServer server;
    private int opCount;
    private int port;

    public StressTestWorker(RaftServer server, int opCount, int port) {
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
                toServer.send(new RaftMessage("Message #" + i));
//                Thread.sleep(10);
//                System.out.println(toServer.receive());
                toServer.receive();
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
