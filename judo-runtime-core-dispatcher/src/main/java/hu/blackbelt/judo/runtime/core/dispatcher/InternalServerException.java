package hu.blackbelt.judo.runtime.core.dispatcher;

public class InternalServerException extends RuntimeException {
	private static final long serialVersionUID = -3620451581905761210L;

	public InternalServerException(final String message) {
        super(message);
    }

    public InternalServerException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
