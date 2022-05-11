
# Pastry-DHT

Pastry-DHT is an implementation of a Distributed Hash Table. The data is kept as Key-Value pairs in a redundant peer-to-peer network that provides a log n look time, similar to Hash Tables.

Each node in the network has an 16-bit identifier, a 4x16 routing table, and a leaf-set. The data (files) are stored in the node with the numerically closest identifier to the data's identifier.

## Files Description

### Wireformats
The protocols required for network inter-node communication are stored here.

### DataStorage.java
Class for Querying data from a Node or pushing data to a Node.
This class will be run by the node that wants to access or alter the data in the cluster.

### DiscoveryNode.java
Provides a random starting node for traversal and resolves same identifier conflicts.

### PastryNode.java
This is the primary node, or class, that runs as a single Pastry Node on various ports. This node handles everything, including data storage, node addition, and traversal.

### CS555-Fall2015.pdf
I used this assignment question as a reference for the entire project.
