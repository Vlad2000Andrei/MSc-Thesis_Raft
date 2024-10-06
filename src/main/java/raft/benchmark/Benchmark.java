package raft.benchmark;

import raft.classic.EchoRaftServer;
import raft.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Node;
import raft.network.SocketConnection;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        System.out.println("Hello world!");
        try {
            stressTestEcho(20, 1_000, 50000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stressTestEcho (int workers, int nOpsPerWorker, int startPort) {
        try {
            System.out.printf("[i] Starting stress test with %s workers, performing %s operations each.\n", workers, nOpsPerWorker);
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", startPort);
            RaftServer server = new EchoRaftServer(serverAddress);
            List<Runnable> runnables = new ArrayList<>();

            new Thread(server::start).start();

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
}
