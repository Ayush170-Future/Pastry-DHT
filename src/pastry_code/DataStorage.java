package pastry_code;

// Class for Querying data from a Node or pushing data to a Node.
// This class will be ran by the node who want to access or alter the data in the cluster.

import pastry_code.wireformats.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class DataStorage {

    // Logging the flow of program.
    private static final Logger LOGGER = Logger.getLogger(DataStorage.class.getCanonicalName());

    public static void main(String[] args) {

        // Port on which this class will run.
        int port = 0;

        // Discovery Node routing information for connection.
        String discoveryNodeAddress = "";
        int  discoveryNodePort = 0;

        try {
            port = Integer.parseInt(args[0]);
            discoveryNodeAddress = args[1];
            discoveryNodePort = Integer.parseInt(args[2]);
        } catch (Exception e) {
            System.out.println("Syntax: DataStorage port DiscoveryNodeAddress DiscoveryNodePort");
            System.exit(1);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String input = "";
        while(true) {
            try {

                System.out.println("Functionality Options \nS) Store Data \nR) Retrieve Data \nQ) Quit!");

                input = br.readLine();
                input = input.substring(1);

                if(input.equalsIgnoreCase("Q")) {
                    break;
                } else if(!input.equalsIgnoreCase("S") || !input.equalsIgnoreCase("R")) {
                    System.out.println("Wrong option!");
                    continue;
                }

                // Storing file.
                if(input.equalsIgnoreCase("S")) {

                    // Getting the file by creating object of File class from the file location.
                    System.out.println("Enter File Name");
                    File file = new File(br.readLine());

                    if(!file.exists()) {
                        throw new Exception("File " +file.getName() +" doesn't exist");
                    }

                    // Getting the ID of the file.
                    byte[] id;
                    System.out.println("Enter the ID of the file(leave black to generate one)");

                    String idString = br.toString();

                    if(idString.length() != 0) {
                        id = HexConverter.convertHexToBytes(idString);
                    } else {
                        short random = (short) (file.getName().hashCode() % (int) Short.MAX_VALUE);
                        id = new byte[2];
                        id[0] = (byte) (random & 0xff);
                        id[1] = (byte) ((random >> 8) & 0xff);
                    }

                    // Fetching a random from discovery node so that we can start the traversal(lookup) from their.
                    NodeAddress randomSeedingNode = getRandomNode(discoveryNodeAddress, discoveryNodePort);

                    // Look up in the cluster; returns the desired Node.
                    NodeAddress nodeAddress = lookup(id, randomSeedingNode, port);

                    // Reading the content of the file into the buffer for storing in byte[] array.
                    FileInputStream infile = new FileInputStream(file);

                    // file.length returns the length of the stream.
                    byte[] data = new byte[(int)file.length()];
                    int len = infile.read(data);

                    if(len != data.length) {
                        LOGGER.severe("Error reading data into the buffer.");
                        return;
                    }

                    infile.close();

                    // Writing data to the desired Node.
                    WriteDataMessage writeDataMessage = new WriteDataMessage(id, data);
                    Socket writeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());

                    ObjectOutputStream out = new ObjectOutputStream(writeSocket.getOutputStream());
                    out.writeObject(writeDataMessage);

                    writeSocket.close();
                    LOGGER.info("Send the file with the ID: " +HexConverter.convertBytesToHex(id) +" to the Node " +nodeAddress);
                }
                else if(input.equalsIgnoreCase("R")) {

                    // Getting ID.
                    System.out.println("Enter ID of the file");
                    String idString = br.readLine();
                    byte[] id = HexConverter.convertHexToBytes(idString);

                    // Get random node.
                    NodeAddress seedNodeAddress = getRandomNode(discoveryNodeAddress, discoveryNodePort);

                    // lookup id in cluster
                    NodeAddress nodeAddress = lookup(id, seedNodeAddress, port);

                    ReadDataMessage readDataMessage = new ReadDataMessage(id);
                    Socket readSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
                    ObjectOutputStream out = new ObjectOutputStream(readSocket.getOutputStream());
                    out.writeObject(readDataMessage);

                    // Parsing reply from the node
                    Protocol reply = (Protocol) new ObjectInputStream(readSocket.getInputStream()).readObject();

                    readSocket.close();

                    if(reply.getMessageType() == Protocol.ERROR_MSG) {
                        throw new Exception(((ErrorMessage) reply).getMessage());
                    } else if(reply.getMessageType() != Protocol.WRITE_DATA_MSG) {
                        throw new Exception("Received an unexpected message type " +reply.getMessageType());
                    }

                    WriteDataMessage writeDataMessage = (WriteDataMessage) reply;

                    // Getting the file name to push the data into it.
                    System.out.println("Enter a file name");
                    File file = new File(br.readLine());

                    FileOutputStream fileOutputStream = new FileOutputStream(file);

                    for(byte data: writeDataMessage.getData()) {
                        fileOutputStream.write(data);
                    }

                    fileOutputStream.close();

                    LOGGER.info("Wrote the data into a file: " +file.getCanonicalPath());
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.severe(e.getMessage());
            }
        }


    }

    public static NodeAddress getRandomNode(String discoveryNodeAddress, int discoveryNodePort) throws Exception {

        Socket socket = new Socket(discoveryNodeAddress, discoveryNodePort);

        RequestRandomNodeMessage requestRandomNodeMessage = new RequestRandomNodeMessage();

        ObjectOutputStream write = new ObjectOutputStream(socket.getOutputStream());

        write.writeObject(requestRandomNodeMessage);

        Protocol read = (Protocol) new ObjectInputStream(socket.getInputStream()).readObject();

        socket.close();

        if(read.getMessageType() == Protocol.ERROR_MSG) {
            throw new Exception(((ErrorMessage)read).getMessage());
        } else if(read.getMessageType() != Protocol.NODE_INFO_MSG) {
            throw new Exception("Received an unexpected message: " +read.getMessageType());
        }

        return ((NodeInformationMessage)read).getNodeAddress();
    }

    public static NodeAddress lookup(byte[] id, NodeAddress seedAddress, int serverPort) throws Exception {

        // Starting a server socket for listening from the nodes.
        ServerSocket serverSocket = new ServerSocket(serverPort);

        LookupNodeMessage lookupNodeMessage = new LookupNodeMessage(id, new NodeAddress("DataStorage", null, serverPort), 0);

        lookupNodeMessage.addHop(seedAddress);

        Socket socket = new Socket(seedAddress.getInetAddress(), seedAddress.getPort());

        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

        // Seeded the random node with the look up message and now the desired node will try to connect this DataStorage.
        out.writeObject(lookupNodeMessage);
        socket.close();

        Socket thisSocket = serverSocket.accept();

        Protocol replyMessage = (Protocol) new ObjectInputStream(thisSocket.getInputStream()).readObject();

        if(replyMessage.getMessageType() == Protocol.ERROR_MSG) {
            throw new Exception(((ErrorMessage)replyMessage).getMessage());
        } else if(replyMessage.getMessageType() != Protocol.NODE_INFO_MSG) {
            throw new Exception("Received a unexpected message type " +replyMessage.getMessageType() +".");
        }

        // Parsing the Reply Message into Desired Node Information.
        NodeInformationMessage information = (NodeInformationMessage) replyMessage;

        thisSocket.close();
        serverSocket.close();
        return information.getNodeAddress();
    }
}
