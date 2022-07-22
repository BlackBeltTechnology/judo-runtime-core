package hu.blackbelt.judo.runtime.core.exception;

import hu.blackbelt.judo.dao.api.ValidationResult;

public class AccessDeniedException extends ClientException {

	private static final long serialVersionUID = -8619727759658375103L;
	private final ValidationResult validationResult;

    public AccessDeniedException(ValidationResult validationResult) {
        super();
        this.validationResult = validationResult;
    }

    @Override
    public Integer getStatusCode() {
        return 403;
    }

    @Override
    public Object getDetails() {
        return validationResult;
    }
}
