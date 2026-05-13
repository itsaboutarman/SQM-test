package ir.mohaymen.sqm.exceptions;

public class InstantiationException extends ReflectiveOperationException{
    public InstantiationException(String message, Throwable cause) {
        super(message, cause);
    }
}