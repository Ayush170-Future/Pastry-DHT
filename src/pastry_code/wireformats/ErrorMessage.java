package pastry_code.wireformats;

public class ErrorMessage extends Protocol{

    // Error Message.
    private String message;

    public ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public int getMessageType() {
        return ERROR_MSG;
    }
}
