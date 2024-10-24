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
import java.util.List;

public class Benchmark {

    public static void main (String[] args) throws InterruptedException {
        switch (args[0].toLowerCase()) {
            case "worker" -> {
                String controllerAddr = args[1];
                Integer controllerPort = Integer.parseInt(args[2]);
                InetSocketAddress controllerSock = new InetSocketAddress(controllerAddr, controllerPort);
                Integer ownId = Integer.parseInt(args[3]);
                String configFilePath = args[4];
                new BenchmarkWorker(controllerSock, configFilePath, ownId).run();
            }
            case "controller", "master" -> {
                String controllerAddr = args[1];
                Integer controllerPort = Integer.parseInt(args[2]);
                new BenchmarkController(controllerAddr, controllerPort).run();
            }
        }
    }
}
