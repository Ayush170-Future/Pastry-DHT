package pastry_code.wireformats;
import pastry_code.NodeAddress;

public class NodeInformationMessage extends Protocol {
    // Node Identifier.
    private byte[] id;
    private NodeAddress nodeAddress;

    NodeInformationMessage(byte[] id, NodeAddress nodeAddress) {
        this.id = id;
        this.nodeAddress = nodeAddress;
    }

    public byte[] getId() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    @Override
    public int getMessageType() {
        return NODE_INFO_MSG;
    }
}
