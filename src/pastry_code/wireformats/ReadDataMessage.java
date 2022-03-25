package pastry_code.wireformats;

public class ReadDataMessage extends Protocol{
    private byte[] id;

    ReadDataMessage(byte[] id) {
        this.id = id;
    }

    public byte[] getId() {
        return id;
    }

    @Override
    public int getMessageType() {
        return READ_DATA_MSG;
    }
}