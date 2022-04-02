package pastry_code;

import java.io.Serializable;
import java.net.InetAddress;

// Note: Node Identifier(id) and Node Address are different.
// Node Identifier is a 16-bit or a 4 digit hex unique id for every Node, while Node Address is all the information about that Node.

public class NodeAddress implements Serializable {

    private String nodeName;
    private InetAddress inetAddress;
    private int port;

    public NodeAddress(String nodeName, InetAddress inetAddress, int port) {
        this.nodeName = nodeName;
        this.inetAddress = inetAddress;
        this.port = port;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setInetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof NodeAddress) {
            NodeAddress nodeAddress = (NodeAddress) obj;
            int hostNameCheck = nodeAddress.getInetAddress().getHostName().compareTo(inetAddress.getHostName());
            int ipCheck = nodeAddress.inetAddress.getHostAddress().compareTo(inetAddress.getHostAddress());
            int portCheck = nodeAddress.getPort() == port ? 0 : 1;

            if(portCheck == 0 && (ipCheck == 0 || hostNameCheck == 0))
                return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return nodeName;
    }

}
