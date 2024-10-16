package raft.benchmark;

import raft.classic.ClassicRaftServer;
import raft.classic.EchoRaftServer;
import raft.messaging.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Configuration;
import raft.network.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        System.out.println("Hello world!");
        try {
            // Benchmark with remote server
//            Node<RaftMessage> server = new Node<>(new InetSocketAddress("192.168.0.106",55000));
//            stressTestEcho(10, 10_000, 50000, server);

            // Benchmark with local server
//            stressTestEcho(4, 100_000, 55000);
            spawn3Servers();
        }


        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stressTestEcho (int workers, int nOpsPerWorker, int startPort, Node<RaftMessage> server) {
        try {
            System.out.printf("[i] Starting stress test with %s workers, performing %s operations each.\n", workers, nOpsPerWorker);

            List<Runnable> runnables = new ArrayList<>();

            for (int i = 1; i <= workers; i++) {
                runnables.add(new StressTestWorker(server, nOpsPerWorker, startPort + i));
            }

            List<Thread> threads = runnables.stream().map(Thread::new).toList();
            threads.forEach(Thread::start);
            for (Thread thread : threads) {
                thread.join();
            }
            Thread.sleep(1000);
            System.out.println("[i] Stress test completed!");
        } catch (Exception e) {
            System.out.println("[ERR] Stress test failed!");
            e.printStackTrace();
        }
    }

    public static void stressTestEcho (int workers, int nOpsPerWorker, int startPort) {
        try {
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", startPort);
            RaftServer server = new EchoRaftServer(serverAddress);
            server.id = 0;
            new Thread(() -> server.start(new Configuration(List.of(server)))).start();

            stressTestEcho(workers, nOpsPerWorker, startPort, server);
        } catch (Exception e) {
            System.out.println("[ERR] Stress test failed!");
            e.printStackTrace();
        }
    }

    public static void spawn2Servers() throws IOException {
        RaftServer server1 = new ClassicRaftServer(new InetSocketAddress("localhost", 55000));
        RaftServer server2 = new ClassicRaftServer(new InetSocketAddress("localhost", 55001));
        server1.id = 0;
        server2.id = 1;

        Configuration config = new Configuration(List.of(server1, server2));
        new Thread(() -> server1.start(config)).start();
        new Thread(() -> server2.start(config)).start();
    }

    public static void spawn3Servers() throws IOException {
        RaftServer server1 = new ClassicRaftServer(new InetSocketAddress("localhost", 55000));
        RaftServer server2 = new ClassicRaftServer(new InetSocketAddress("localhost", 55001));
        RaftServer server3 = new ClassicRaftServer(new InetSocketAddress("localhost", 55002));
        server1.id = 0;
        server2.id = 1;
        server3.id = 2;

        Configuration config = new Configuration(List.of(server1, server2, server3));
        new Thread(() -> server1.start(config)).start();
        new Thread(() -> server2.start(config)).start();
        new Thread(() -> server3.start(config)).start();
    }

    public static void spawn5Servers() throws IOException {
        RaftServer server1 = new ClassicRaftServer(new InetSocketAddress("localhost", 55000));
        RaftServer server2 = new ClassicRaftServer(new InetSocketAddress("localhost", 55001));
        RaftServer server3 = new ClassicRaftServer(new InetSocketAddress("localhost", 55002));
        RaftServer server4 = new ClassicRaftServer(new InetSocketAddress("localhost", 55003));
        RaftServer server5 = new ClassicRaftServer(new InetSocketAddress("localhost", 55004));
        server1.id = 0;
        server2.id = 1;
        server3.id = 2;
        server4.id = 3;
        server5.id = 4;

        Configuration config = new Configuration(List.of(server1, server2, server3, server4, server5));
        new Thread(() -> server1.start(config)).start();
        new Thread(() -> server2.start(config)).start();
        new Thread(() -> server3.start(config)).start();
        new Thread(() -> server4.start(config)).start();
        new Thread(() -> server5.start(config)).start();
    }
}
