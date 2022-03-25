package pastry_code.wireformats;

public class RequestRandomNodeMessage extends Protocol{

    @Override
    public int getMessageType() {
        return REQUEST_RANDOM_NODE;
    }
}
