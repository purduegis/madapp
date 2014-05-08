package exceptions;

public class MetadataException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public MetadataException() {
        super();
    }
    public MetadataException(String msg) {
        super(msg);
    }
    
    public MetadataException(Throwable cause) {
        super(cause);
    }
    
    public MetadataException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
