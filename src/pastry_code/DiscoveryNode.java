package pastry_code;

import pastry_code.wireformats.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class DiscoveryNode extends Thread {
    private static final Logger LOGGER = Logger.getLogger(DiscoveryNode.class.getCanonicalName());
    protected int port;
    protected Map<byte[], NodeAddress> nodes;
    protected ReadWriteLock readWriteLock;
    private final Random random;

    public DiscoveryNode(int port) {
        this.port = port;
        random = new Random();
        nodes = new HashMap<>();
        readWriteLock = new ReentrantReadWriteLock();
    }

    public static void main(String[] args) {
        try {
            if (args.length != 1) throw new Exception();
            int port = Integer.parseInt(args[0]);

            Thread discoveryNodeThread = new Thread(new DiscoveryNode(port));
            discoveryNodeThread.start();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Syntax: DiscoveryNode port");
        }
    }

    public boolean isNodesListEmpty() {
        readWriteLock.readLock().lock();
        try {
            return nodes.isEmpty();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public byte[] getRandomNode() {
        readWriteLock.readLock().lock();
        try {
            int randomID = random.nextInt(nodes.size());

            for(byte[] ID: nodes.keySet()) {
                if(randomID-- == 0) {
                    return ID;
                }
            }
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void addNode(byte[] id, NodeAddress nodeAddress) throws Exception{
        readWriteLock.readLock().lock();

        // Check if the ID is already present in the network or not?
        try {
            if(nodes.containsKey(id)) {
                throw new Exception("ID " +HexConverter.convertBytesToHex(id) +" is already present in the Network");
            }
        } finally {
            readWriteLock.readLock().unlock();
        }

        // Adding the node with the given ID in the network.
        readWriteLock.readLock().lock();
        try {
            nodes.put(id, nodeAddress);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected void printActiveNodes() {
        readWriteLock.readLock().lock();
        try {
            StringBuilder str = new StringBuilder("----ACTIVE NODES----");
            for(Map.Entry<byte[],NodeAddress> entry : nodes.entrySet()) {
                str.append("\n").append(HexConverter.convertBytesToHex(entry.getKey())).append(" : ").append(entry.getValue());
            }
            str.append("\n--------------------");
            LOGGER.info(str.toString());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void run() {
        try {
            // Creating a Server Socket for receiving the requests from the network.
            ServerSocket serverSocket = new ServerSocket(port);

            // Accepts Connection.
            while(true) {
                Socket socket = serverSocket.accept();
                System.out.println("Received Connection from: " +socket.getInetAddress() +":" +socket.getPort() +".");

                Thread subThread = new Thread(new DiscoveryNodeWorker(socket));
                subThread.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // This is the Thread which takes the Node Request and answer accordingly.
    private class DiscoveryNodeWorker extends Thread {
        protected Socket socket;

        public DiscoveryNodeWorker(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {

            try {
                // Reading request message from the connection.
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                Protocol requestMessage = (Protocol) input.readObject();  // Compile-time casting.
                Protocol replyMessage = null;

                switch (requestMessage.getMessageType()) {

                    case Protocol.REGISTER_NODE_MSG:

                        RegisterNodeMessage registerNodeMessage = (RegisterNodeMessage) requestMessage;  // Down-casting to the subclass object for using the subclass specific methods.
                        try {
                            readWriteLock.readLock().unlock();

                            // If this new node is the first node in the cluster then don't need to do anything.
                            if(isNodesListEmpty()) {
                                replyMessage = new SuccessMessage();
                            } else {

                                // Else we will pass a Random node to the new node for adding it's self in the network.
                                byte[] randomID = getRandomNode();

                                replyMessage = new NodeInformationMessage(randomID, nodes.get(randomID));
                            }

                            addNode(registerNodeMessage.getId(), new NodeAddress(registerNodeMessage.getNodeName(),
                                    socket.getInetAddress(), registerNodeMessage.getPort()));

                        } catch (Exception e) {
                            replyMessage = new ErrorMessage(e.getMessage());
                        } finally {
                            readWriteLock.readLock().unlock();
                        }

                        // Print the active Nodes.
                        printActiveNodes();
                        break;

                    case Protocol.REQUEST_RANDOM_NODE:

                        readWriteLock.readLock().lock();
                        try {
                            if(isNodesListEmpty()) {

                                replyMessage = new ErrorMessage("The list of Nodes is empty");

                            } else {

                                byte[] id = getRandomNode();
                                replyMessage = new NodeInformationMessage(id, nodes.get(id));

                            }
                        } catch (Exception e) {
                            replyMessage = new ErrorMessage(e.getMessage());
                        } finally {
                            readWriteLock.readLock().unlock();
                        }
                        break;

                    default:
                        LOGGER.severe("Unrecognized request message type '" + requestMessage.getMessageType() + "'");
                        break;
                }

                // Sending the reply message to the calling Node.
                if(replyMessage != null) {
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(replyMessage);
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                LOGGER.severe(e.getMessage());
            }
        }
    }
}
