package pastry_code.wireformats;

import java.net.InetAddress;

// Message for the Discovery Node to Register the information about this peer into it's storage.

public class RegisterNodeMessage extends Protocol {

    private final String nodeName;
    private final byte[] id;
    private final InetAddress inetAddress;
    private final int port;

    RegisterNodeMessage(String nodeName, byte[] id, InetAddress inetAddress, int port) {
        this.nodeName = nodeName;
        this.id = id;
        this.inetAddress = inetAddress;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public byte[] getId() {
        return id;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public String getNodeName() {
        return nodeName;
    }

    @Override
    public int getMessageType() {
        return REGISTER_NODE_MSG;
    }
}
