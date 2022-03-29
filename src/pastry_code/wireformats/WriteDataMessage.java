package pastry_code.wireformats;

public class WriteDataMessage extends Protocol {
    private byte[] id;
    private byte[] data;

    public WriteDataMessage(byte[] id, byte[] data) {
        this.id = id;
        this.data = data;
    }

    public byte[] getId() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public int getMessageType() {
        return WRITE_DATA_MSG;
    }
}
