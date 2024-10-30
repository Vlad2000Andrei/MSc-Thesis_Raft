package raft.benchmark;

import raft.common.LogEntry;
import raft.common.RaftLog;
import raft.common.RaftServer;
import raft.messaging.common.RaftMessage;
import raft.network.Configuration;
import raft.network.Node;
import raft.servers.ClassicRaftServer;
import raft.servers.ModifiedRaftServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class BenchmarkWorker implements Runnable {
    protected enum WorkerType {
        CLASSIC,
        MODIFIED
    }

    private final Socket controllerSocker;
    private final InetSocketAddress controllerAddress;

    private ObjectInputStream fromController = null;
    private ObjectOutputStream toController = null;
    private WorkerType workerType;
    private String configFilePath;
    private int ownId;
    private RaftServer server;

    public BenchmarkWorker(InetSocketAddress controllerAddress, String configFilePath, int ownId) {
        controllerSocker = new Socket();

        this.configFilePath = configFilePath;
        this.ownId = ownId;
        this.controllerAddress = controllerAddress;
        try {
            controllerSocker.connect(this.controllerAddress);
            fromController = new ObjectInputStream(controllerSocker.getInputStream());
            toController = new ObjectOutputStream(controllerSocker.getOutputStream());
            toController.writeObject(new BenchmarkControlMessage(BenchmarkControlMessageType.HELLO, null, ownId));
        }
        catch (IOException ioException) {
            System.out.println("Could not connect to controller, exiting.");
            System.exit(1);
        }
    }

    private void startServer() {
        try {
            System.out.printf("[ModifiedRaftServer] Opening config file %s\n", configFilePath);
            File configFile = new File(configFilePath);
            FileInputStream fis = new FileInputStream(configFile);
            Scanner fileScanner = new Scanner(fis);

            List<Node<RaftMessage>> peers = new ArrayList<>();

            while (fileScanner.hasNextLine()) {
                String line = fileScanner.nextLine();
                String[] peerDetails = line.split(" ");

                int peerId = Integer.parseInt(peerDetails[0]);
                String peerAddress = peerDetails[1];
                int peerPort = Integer.parseInt(peerDetails[2]);

                if (peerId == ownId) {
                    server = switch (workerType) {
                        case MODIFIED ->  new ModifiedRaftServer(new InetSocketAddress(peerAddress, peerPort));
                        case CLASSIC -> new ClassicRaftServer(new InetSocketAddress(peerAddress, peerPort));
                    };

                    server.id = ownId;
                    peers.add(server);
                }
                else {
                    peers.add(new Node<RaftMessage>(new InetSocketAddress(peerAddress, peerPort)).setId(peerId));
                }
            }
            fis.close();

            Configuration cluster = new Configuration(peers);
            server.start(cluster);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void run() {
        Thread serverThread = new Thread(this::startServer);
        serverThread.setDaemon(true);
        serverThread.setName("Server " + ownId);

        while (true) {
            try {
                BenchmarkControlMessage msg = (BenchmarkControlMessage) fromController.readObject();
                switch (msg.type()) {
                    case START_CLASSIC -> {
                        workerType = WorkerType.CLASSIC;
                        serverThread.start();
                    }
                    case START_MODIFIED -> {
                        workerType = WorkerType.MODIFIED;
                        serverThread.start();
                    }
                    case LOG -> {
                        System.out.println("Returning log to controller...");
                        List<LogEntry> logEntries = server.getLog().getEntries();
                        BenchmarkControlMessage response = new BenchmarkControlMessage(BenchmarkControlMessageType.LOG_OK, logEntries, server.id);
                        toController.writeObject(response);
                    }
                    case STOP -> {
                        System.out.println("Stopping...");
                        BenchmarkControlMessage response = new BenchmarkControlMessage(BenchmarkControlMessageType.STOP_OK, null, server.id);
                        System.exit(0);
                    }
                    case CRASH -> {
                        if (server != null) server.crashNow = true;
                    }
                }
            }
            catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
