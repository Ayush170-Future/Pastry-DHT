package pastry_code;

import pastry_code.wireformats.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class PastryNode extends Thread {
    protected String nodeName;
    protected byte[] id;
    protected int port;
    protected short idValue;
    protected String idString;
    protected String discoveryNodeAddress;
    protected int discoveryNodePort;
    protected String storageDirectory;
    protected ReadWriteLock readWriteLock;
    protected HashMap<String, NodeAddress>[] routingTable;
    //protected TreeMap<byte[], NodeAddress> lessThanLS, greaterThanLS;
    protected leafSet lowLeaf;
    protected leafSet highLeaf;
    protected List<String> dataStores;

    protected static class leafSet {
        String id;
        NodeAddress nodeAddress;
        public leafSet(String id, NodeAddress nodeAddress) {
            this.id = id;
            this.nodeAddress = nodeAddress;
        }
    }

    private static final Logger LOGGER = Logger.getLogger(PastryNode.class.getCanonicalName());

    public PastryNode(String nodeName, byte[] id, int port, String discoveryNodeAddress, int discoveryNodePort, String storageDirectory) {
        this.nodeName = nodeName;
        this.id = id;
        this.port = port;
        this.discoveryNodeAddress = discoveryNodeAddress;
        this.discoveryNodePort = discoveryNodePort;
        this.storageDirectory = storageDirectory;
        // String representation of the ID, for the ease in lookups.
        idString = HexConverter.convertBytesToHex(id);
        idValue = byteToShort(id);

//        lessThanLS = new TreeMap<>((byte[] a, byte[] b) -> {
//           int d1 = lessThanDistance(byteToShort(a), idValue);
//           int d2 = lessThanDistance(byteToShort(b), idValue);
//
//           return d2 - d1;
//        });
//
//        greaterThanLS = new TreeMap<>((byte[] a, byte[] b) -> {
//            int d1 = greaterThanDistance(byteToShort(a), idValue);
//            int d2 = greaterThanDistance(byteToShort(b), idValue);
//
//            return d1 - d2;
//        });

        lowLeaf = new leafSet("", null);
        highLeaf = new leafSet("", null);

        // Initializing the 4x16 routing table.
        routingTable = new HashMap[4];
        for(int i = 0; i < 4; i++) {
            routingTable[i] = new HashMap<>();
        }

        dataStores = new LinkedList<>();

        // Initializing the Thread read write lock mechanism.
        readWriteLock = new ReentrantReadWriteLock();
    }

    public static void main(String[] args) {
        try {
            String nodeName = args[0];
            String storageDirectory = args[1];
            String discoveryNodeAddress = args[2];
            int discoveryNodePort = Integer.parseInt(args[3]);
            int port = Integer.parseInt(args[4]);
            byte[] id = args.length == 6 ? HexConverter.convertHexToBytes(args[5]) : generateID();

            // Running this Node on a thread.
            Thread thisNodeThread = new Thread(new PastryNode(nodeName, id, port, discoveryNodeAddress, discoveryNodePort, storageDirectory));

            thisNodeThread.start();

        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
            System.out.println("Syntax: PastryNode NodeName StorageDirectory DiscoveryNodeAddress DiscoveryNodePort Port ID");
        }
    }

    @Override
    public void run() {
        try {

            // Initializing the Sever Socket for this Node, for listening the connections on this port.
            ServerSocket listeningSocket = new ServerSocket(port);

            // Registering this Node into the Discovery Node and the cluster.
            boolean success = false;

            while(!success) {
                //success = true;
                LOGGER.info("Registering ID: " +HexConverter.convertBytesToHex(id) +" to the Discovery Node: "
                        +discoveryNodeAddress +":" +discoveryNodePort);

                Socket discoveryNodeSocket = new Socket(discoveryNodeAddress, discoveryNodePort);

                // Register Message.
                RegisterNodeMessage registerNodeMessage = new RegisterNodeMessage(nodeName, id, listeningSocket.getInetAddress(), port);
                ObjectOutputStream out = new ObjectOutputStream(discoveryNodeSocket.getOutputStream());
                out.writeObject(registerNodeMessage);

                // Reply from the Discovery Node.
                ObjectInputStream in = new ObjectInputStream(discoveryNodeSocket.getInputStream());
                Protocol reply = (Protocol) in.readObject();
                discoveryNodeSocket.close();

                // Actions based upon the reply message.
                switch (reply.getMessageType()) {
                    case Protocol.SUCCESS_MSG:
                        // Then this node is the first node in the cluster.
                        success = true;
                        break;

                    case Protocol.ERROR_MSG:
                        LOGGER.severe(((ErrorMessage)reply).getMessage());
                        id = generateID();
                        continue;

                    case Protocol.NODE_INFO_MSG:
                        // Now we have to call the given node and publish this node into the cluster.

                        NodeInformationMessage nodeInformationMessage = (NodeInformationMessage) reply;

                        LOGGER.info("Sending Node Join Message to " +nodeInformationMessage.getNodeAddress().getInetAddress()
                                +":" +nodeInformationMessage.getNodeAddress().getPort());

                        NodeJoinMessage nodeJoinMessage = new NodeJoinMessage(id, 0,
                                new NodeAddress(nodeName, listeningSocket.getInetAddress(), port));

                        nodeJoinMessage.addHop(nodeInformationMessage.getNodeAddress());

                        // Providing the seeding node with the information of this node for joining.
                        Socket nodeSocket = new Socket(nodeInformationMessage.getNodeAddress().getInetAddress(),
                                nodeInformationMessage.getNodeAddress().getPort());
                        ObjectOutputStream writeJoinMessage = new ObjectOutputStream(nodeSocket.getOutputStream());
                        writeJoinMessage.writeObject(nodeJoinMessage);

                        success = true;
                        break;

                    default:
                        LOGGER.severe("Received an unexpected message type, " +reply.getMessageType());
                        break;
                }
            }

            // Now the node has been registered in the cluster.
            // And now this Node will start receiving connections from other nodes in the cluster.

            while(true) {
                Socket socket = listeningSocket.accept();
                LOGGER.info("Received connection from " +socket.getInetAddress() +":" +socket.getPort());

                // Making a new Worker class that will run on a separate thread specifically for this connection.
                Thread worker = new Thread(new PastryNodeWorker(socket));
                worker.start();
            }


        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.severe(e.getMessage());
        }
    }

    // Worker class.
    protected class PastryNodeWorker extends Thread {
        protected Socket socket;

        public PastryNodeWorker(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                // Fetching the request message from the socket.
                ObjectInputStream inputStream =  new ObjectInputStream(socket.getInputStream());
                Protocol requestMessage = (Protocol) inputStream.readObject();

                // Initializing the reply message for the request.
                Protocol reply;

                switch (requestMessage.getMessageType()) {
                    case Protocol.NODE_JOIN_MSG:
                        // Request for joining a new node.

                        NodeJoinMessage nodeJoinMessage = (NodeJoinMessage) requestMessage;
                        LOGGER.info("Received Node Join Message " +nodeJoinMessage.toString() +".");

                        int prefixMatch = nodeJoinMessage.getPrefixLength();

                        // Search for a exact match in the routing table.
                        NodeAddress matchingNode = exactMatchInRoutingTable(nodeJoinMessage.getId(), prefixMatch);

                        // If we find a match.
                        if(matchingNode != null) {
                            nodeJoinMessage.setLongestPrefixLength(prefixMatch + 1);
                        }

                        // Else if we don't find a matching node, we will get the closest
                        // node in the routing table or in the leaf set and continue the process.
                        if(matchingNode == null) {
                            matchingNode = closestMatchingInRoutingTable(nodeJoinMessage.getId(), prefixMatch);
                        }

                        // finding closest node in the leafSet.
                        if(matchingNode == null || nodeJoinMessage.hopContains(matchingNode)) {

                        }

                }

            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.severe(e.getMessage());
            }

        }
    }


    protected NodeAddress exactMatchInRoutingTable(byte[] id, int prefix) {
        readWriteLock.readLock().lock();
        try {
            return routingTable[prefix].get(HexConverter.convertBytesToHex(id).substring(prefix, prefix+1));
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected NodeAddress closestMatchingInRoutingTable(byte[] id, int prefix) {
        readWriteLock.readLock().lock();
        try {
            String nodeIDString = HexConverter.convertBytesToHex(id);
            // we are only interested in the prefix 'th char in the nodeIDString, for the routing searches.
            String interestedNodeChar = nodeIDString.substring(prefix, prefix+1);
            String interestedThisNodeChar = idString.substring(prefix, prefix+1);

            int minDiff = Math.abs(Integer.parseInt(interestedNodeChar, 16) - Integer.parseInt(interestedThisNodeChar, 16));
            short minValue = (short) Integer.parseInt(interestedThisNodeChar, 16);

            NodeAddress closestNode = null;

            for(String key: routingTable[prefix].keySet()) {
                int diff = Math.abs(Integer.parseInt(key, 16) - Integer.parseInt(interestedNodeChar, 16));
                short value = (short) Integer.parseInt(key, 16);

                if(diff < minDiff || (diff == minDiff && value > minValue)) {
                    minDiff = diff;
                    minValue = value;
                    closestNode = routingTable[prefix].get(key);
                }
            }

            return closestNode;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected static byte[] generateID() {
        Random random = new Random();

        // byte[] size for ID is defaulted to be 2 coz the ID length is 16 bits.
        byte[] id = new byte[2];

        for(int i = 0; i < 2; i++) {
            id[i] = (byte) (random.nextInt() % 256);
        }

        return id;
    }

    protected int lessThanDistance(short s1, short s2) {
        if(s1 <= s2) {
            return s2 - s1;
        } else {
            return Short.MAX_VALUE - s1 + s2 - Short.MIN_VALUE;
        }
    }

    protected int greaterThanDistance(short s1, short s2) {
        if(s1 >= s2) {
            return s1 - s2;
        } else {
            return s1 - Short.MIN_VALUE + Short.MAX_VALUE - s2;
        }
    }

    protected short byteToShort(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b);
        return bb.getShort(0);
    }
}