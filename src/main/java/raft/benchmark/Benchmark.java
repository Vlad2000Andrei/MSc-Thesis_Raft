package raft.benchmark;

import raft.classic.ClassicRaftServer;
import raft.classic.EchoRaftServer;
import raft.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Node;
import raft.network.SocketConnection;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        System.out.println("Hello world!");

        try {
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", 55000);
            RaftServer server = new EchoRaftServer(serverAddress);
            new Thread(server::start).start();

            new Thread(() -> Benchmark.stressTest(server, 55000)).start();
            new Thread(() -> Benchmark.stressTest(server, 55001)).start();
            new Thread(() -> Benchmark.stressTest(server, 55002)).start();
            new Thread(() -> Benchmark.stressTest(server, 55003)).start();
            new Thread(() -> Benchmark.stressTest(server, 55004)).start();
            new Thread(() -> Benchmark.stressTest(server, 55005)).start();
            new Thread(() -> Benchmark.stressTest(server, 55006)).start();
//            new Thread(() -> Benchmark.stressTest(server, 55007)).start();
//            new Thread(() -> Benchmark.stressTest(server, 55008)).start();
//            new Thread(() -> Benchmark.stressTest(server, 55009)).start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stressTest (RaftServer server, int port)  {
        System.out.println(Thread.currentThread().getName() + " starting stress test...");
        try {
            InetSocketAddress clientAddress = new InetSocketAddress("localhost", port);
            Node<RaftMessage> client = new Node<>(clientAddress);
            SocketConnection toServer = client.connectTo(server);

            for (int i = 0; i < 1000000; i++) {
                toServer.send(new RaftMessage("Message #" + i));
//                System.out.println(toServer.receive());
                toServer.receive();
            }
            System.out.println(Thread.currentThread().getName() + " finished stress test!");
        }
        catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + " failed stress test!");
            throw new RuntimeException(e);
        }
    }

}
