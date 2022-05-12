package hu.blackbelt.judo.runtime.core.exception;

public abstract class ClientException extends RuntimeException {

	private static final long serialVersionUID = -8702561328947134480L;

	public ClientException() {
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(Throwable cause) {
        super(cause);
    }

    public ClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public abstract Integer getStatusCode();

    public abstract Object getDetails();
}
