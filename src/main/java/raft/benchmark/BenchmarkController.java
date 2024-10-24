package raft.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import raft.common.LogEntry;
import raft.network.Configuration;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class BenchmarkController implements Runnable {
    private final Map<Integer, ObjectOutputStream> outputStreams;
    private ServerSocket serverSocket;
    private final ObjectMapper mapper;

    public BenchmarkController(String bindAddress, Integer bindPort) {
        outputStreams = new ConcurrentHashMap<>();
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        try {
            InetSocketAddress socketAddress = new InetSocketAddress(bindAddress, bindPort);
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            System.out.printf("Controller started on %s.\n", socketAddress);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(1);
        }
    }

    private void handleConnection(ObjectInputStream fromWorker, ObjectOutputStream toWorker) {
        Runnable handler = () -> {
            Integer serverId = null;
            while(true) {
                try {
                    BenchmarkControlMessage msg = (BenchmarkControlMessage) fromWorker.readObject();
                    switch (msg.type()) {
                        case LOG_OK -> {
                            writeLogEntries(msg);
                            System.out.printf("Server %d: %s\n", msg.serverId(), msg.logEntries().toString());
                        }
                        case STOP_OK -> {
                            outputStreams.remove(msg.serverId());
                            System.out.printf("Server %d stopped.\n", msg.serverId());
                            return;
                        }
                        case HELLO -> {
                            outputStreams.put(msg.serverId(), toWorker);
                            serverId = msg.serverId();
                        }
                    }
                }
                catch (SocketException se) {
                    outputStreams.remove(serverId);
                    System.out.printf("Lost connection to server %d, %d remaining.\n", serverId, outputStreams.size());
                    return;
                }
                catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        };
        Thread handlerThread = new Thread(handler);
        handlerThread.setDaemon(true);
        handlerThread.setName("Handler");
        handlerThread.start();
    }

    private void acceptConnections() {
        while(true) {
            try {
                System.out.println("Controller waiting for incoming connections...");
                Socket sock = serverSocket.accept();
                ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                handleConnection(ois, oos);
                System.out.printf("Accepted connection from benchmark worker at %s:%s.\n", sock.getInetAddress().getHostAddress(), sock.getPort());
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private void writeLogEntries (BenchmarkControlMessage logOkMessage) {
        String filePath = String.format("./Server%d_Log.json", logOkMessage.serverId());
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            mapper.writeValue(fos, logOkMessage.logEntries());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        Thread acceptingConnectionsThread = new Thread(this::acceptConnections);
        acceptingConnectionsThread.setDaemon(true);
        acceptingConnectionsThread.start();

        Scanner consoleScanner = new Scanner(System.in);
        while(true) {
            String line = consoleScanner.nextLine();
            try {
                System.out.println("Command: " + line);
                switch (line.toLowerCase()) {
                    case "stop" -> {
                        System.out.printf("Stopping all %d workers.\n", outputStreams.size());
                        for (ObjectOutputStream stream : outputStreams.values()) {
                            stream.writeObject(new BenchmarkControlMessage(BenchmarkControlMessageType.STOP,null,null));
                        }
                    }
                    case "log", "logs" -> {
                        System.out.printf("Requesting logs of all %d workers.\n", outputStreams.size());
                        for (ObjectOutputStream stream : outputStreams.values()) {
                            stream.writeObject(new BenchmarkControlMessage(BenchmarkControlMessageType.LOG,null,null));
                        }
                    }
                    case "start c", "start classic" -> {
                        System.out.printf("Starting classic Raft on all %d workers.\n", outputStreams.size());
                        for (ObjectOutputStream stream : outputStreams.values()) {
                            stream.writeObject(new BenchmarkControlMessage(BenchmarkControlMessageType.START_CLASSIC,null,null));
                        }
                    }
                    case "start m", "start modif", "start modified" -> {
                        System.out.printf("Startin modified Raft on all %d workers.\n", outputStreams.size());
                        for (ObjectOutputStream stream : outputStreams.values()) {
                            stream.writeObject(new BenchmarkControlMessage(BenchmarkControlMessageType.START_MODIFIED,null,null));
                        }
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
