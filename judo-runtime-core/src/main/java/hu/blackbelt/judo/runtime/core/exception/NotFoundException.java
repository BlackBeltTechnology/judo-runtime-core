package hu.blackbelt.judo.runtime.core.exception;

import hu.blackbelt.judo.dao.api.ValidationResult;

public class NotFoundException extends ClientException {

	private static final long serialVersionUID = -153898287660056859L;
	private final ValidationResult validationResult;

    public NotFoundException(ValidationResult validationResult) {
        super();
        this.validationResult = validationResult;
    }

    @Override
    public Integer getStatusCode() {
        return 404;
    }

    @Override
    public Object getDetails() {
        return validationResult;
    }
}
