package exceptions;

public class TwitterException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public TwitterException() {
        super();
    }
    
    public TwitterException(String msg) {
        super(msg);
    }
    
    public TwitterException(Throwable cause) {
        super(cause);
    }
    
    public TwitterException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
