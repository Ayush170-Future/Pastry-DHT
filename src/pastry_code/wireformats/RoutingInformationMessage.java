package pastry_code.wireformats;

import pastry_code.NodeAddress;

import java.util.Map;

// Message for the Routing Information i.e. Leaf-set and Routing table.

public class RoutingInformationMessage extends Protocol{
    private Map<byte[], NodeAddress> leafSet;
    private Map<String, NodeAddress> routingTable;
    private int prefixLength;
    private boolean broadcastMessage;

    RoutingInformationMessage(Map<byte[], NodeAddress> leafSet, int prefixLength, Map<String, NodeAddress> routingTable, boolean broadcastMessage) {
        this.leafSet = leafSet;
        this.prefixLength = prefixLength;
        this.routingTable = routingTable;
        this.broadcastMessage = broadcastMessage;
    }

    @Override
    public int getMessageType() {
        return ROUTING_INFO_MSG;
    }

    public Map<byte[], NodeAddress> getLeafSet() {
        return leafSet;
    }

    public Map<String, NodeAddress> getRoutingTable() {
        return routingTable;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public boolean isBroadcastMessage() {
        return broadcastMessage;
    }
}
