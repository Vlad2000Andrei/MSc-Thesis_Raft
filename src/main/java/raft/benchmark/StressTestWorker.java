package raft.benchmark;

import raft.messaging.common.RaftMessage;
import raft.network.Node;
import raft.network.SocketConnection;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;

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
            Instant startTime = Instant.now();
            Instant currentIntervalStart = Instant.now();
            for (int i = 1; i <= opCount; i++) {
                String content = "Message #" + i;
//                toServer.send(new RaftMessage(content));
                // TODO fix these
//                if (!toServer.receive().message.equals(content)) {
//                    throw new RuntimeException("Received message does not match sent message!");
//                }

                if (i % progressStepSize == 0) {
                    double secondsElapsed = Duration.between(currentIntervalStart, Instant.now()).toMillis() / 1000.0;
                    double opsPerSec = progressStepSize / secondsElapsed;
                    System.out.printf("%s:\t %2.2f%% done. (%.3f Ops/sec over %.2f sec)\n", Thread.currentThread().getName(), i / (double)progressStepSize * 10, opsPerSec, secondsElapsed);
                    currentIntervalStart = Instant.now();
                }
            }
            double secondsElapsedTotal = Duration.between(startTime, Instant.now()).toMillis() / 1000.0;
            double opsPerSecTotal = opCount / secondsElapsedTotal;

            toServer.close();
            System.out.printf("%s finished stress test! Average of %.3f Ops/sec over %.2f sec.\n", Thread.currentThread().getName(), opsPerSecTotal, secondsElapsedTotal);
        }
        catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + " failed stress test!");
            throw new RuntimeException(e);
        }
    }
}
