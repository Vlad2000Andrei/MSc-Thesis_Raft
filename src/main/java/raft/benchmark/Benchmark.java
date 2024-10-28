package raft.benchmark;

import raft.servers.ClassicRaftServer;
import raft.servers.EchoRaftServer;
import raft.messaging.common.RaftMessage;
import raft.common.RaftServer;
import raft.network.Configuration;
import raft.network.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        switch (args[0].toLowerCase()) {
            case "worker" -> {
                System.out.printf("Starting worker id %s controlled by controller at %s:%s.\n", args[3], args[1], args[2]);
                String controllerAddr = args[1];
                Integer controllerPort = Integer.parseInt(args[2]);
                InetSocketAddress controllerSock = new InetSocketAddress(controllerAddr, controllerPort);
                Integer ownId = Integer.parseInt(args[3]);
                String configFilePath = args[4];
                new BenchmarkWorker(controllerSock, configFilePath, ownId).run();
            }
            case "controller", "master" -> {
                System.out.printf("Starting controller at %s:%s.\n", args[1], args[2]);
                String controllerAddr = args[1];
                Integer controllerPort = Integer.parseInt(args[2]);

                // Optional arguments to start a hands-off run
                if (args.length > 3) {
                    Integer workerCount = Integer.parseInt(args[3]);
                    Integer timeLimitMinutes = Integer.parseInt(args[4]);
                    String raftServerType = args[5];
                    new BenchmarkController(controllerAddr, controllerPort, workerCount, timeLimitMinutes, raftServerType).run();

                }
                else {
                    new BenchmarkController(controllerAddr, controllerPort).run();
                }
            }
        }
    }
}
