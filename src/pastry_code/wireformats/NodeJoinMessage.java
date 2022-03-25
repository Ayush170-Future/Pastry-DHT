package pastry_code.wireformats;

import pastry_code.HexConverter;
import pastry_code.NodeAddress;

import java.util.LinkedList;
import java.util.List;

// Message for joining the Node into the network. This message is intended for the node with the same prefix up to a
// particular hop count.

public class NodeJoinMessage extends Protocol {
    private byte[] id;
    private NodeAddress nodeAddress;
    private int prefixLength;
    private List<NodeAddress> hops;  // List for storing the list of nodes in the path.

    NodeJoinMessage(byte[] id, int prefixLength, NodeAddress nodeAddress) {
        this.id = id;
        this.prefixLength = prefixLength;
        this.nodeAddress = nodeAddress;
        hops = new LinkedList<>();
    }

    public byte[] getId() {
        return id;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public void setLongestPrefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    public void addHop(NodeAddress nodeAddress) {
        hops.add(nodeAddress);
    }

    public boolean hopContains(NodeAddress nodeAddress) {
        return hops.contains(nodeAddress);
    }

    @Override
    public int getMessageType() {
        return NODE_JOIN_MSG;
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append("ID: ").append(HexConverter.convertBytesToHex(id));
        string.append("Hops ").append(hops.size());
        string.append("Path: ").append(nodeAddress.toString());

        for(NodeAddress hop: hops) {
            string.append(" -> ").append(hop.toString());
        }

        return string.toString();
    }
}
