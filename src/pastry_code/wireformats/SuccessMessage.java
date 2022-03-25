package pastry_code.wireformats;

public class SuccessMessage extends Protocol {
    @Override
    public int getMessageType() {
        return SUCCESS_MSG;
    }
}
