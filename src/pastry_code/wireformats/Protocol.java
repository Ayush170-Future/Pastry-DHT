package pastry_code.wireformats;

import java.io.Serializable;

public abstract class Protocol implements Serializable {

    // These are messages, that will be used by the peers inside the network to communicate.
    // These don't follow a particular scheme.

    // State-Messages, 100s.
    public static final int ERROR_MSG = 100;
    public static final int SUCCESS_MSG = 101;

    // Querying & Adding files to the Network Messages, 200s.
    public static final int REGISTER_NODE_MSG = 201;
    public static final int NODE_INFO_MSG = 202;
    public static final int NODE_JOIN_MSG = 203;
    public static final int ROUTING_INFO_MSG = 204;
    public static final int LOOKUP_NODE_MSG = 205;
    public static final int REQUEST_RANDOM_NODE = 206;
    public static final int WRITE_DATA_MSG = 207;
    public static final int READ_DATA_MSG = 208;


    // Getter function for the type of the message.
    public abstract int getMessageType();

}
