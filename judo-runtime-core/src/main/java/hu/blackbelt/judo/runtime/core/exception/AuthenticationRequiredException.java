package hu.blackbelt.judo.runtime.core.exception;

import hu.blackbelt.judo.dao.api.ValidationResult;

public class AuthenticationRequiredException extends ClientException {

	private static final long serialVersionUID = -5826001649836508910L;
	private final ValidationResult validationResult;

    public AuthenticationRequiredException(ValidationResult validationResult) {
        super();
        this.validationResult = validationResult;
    }

    @Override
    public Integer getStatusCode() {
        return 401;
    }

    @Override
    public Object getDetails() {
        return validationResult;
    }
}
