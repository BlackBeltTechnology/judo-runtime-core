package hu.blackbelt.judo.runtime.core.dispatcher;

public class InternalServerException extends RuntimeException {

    public InternalServerException(final String message) {
        super(message);
    }

    public InternalServerException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
