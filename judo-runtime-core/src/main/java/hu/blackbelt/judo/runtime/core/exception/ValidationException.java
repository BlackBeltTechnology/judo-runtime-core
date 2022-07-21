package hu.blackbelt.judo.runtime.core.exception;

import hu.blackbelt.judo.dao.api.ValidationResult;
import lombok.Getter;

import java.util.Collection;

public class ValidationException extends ClientException {

	private static final long serialVersionUID = 1550702685342981741L;

	@Getter
    private final Collection<ValidationResult> validationResults;

    public ValidationException(final Collection<ValidationResult> validationResults) {
        super();
        this.validationResults = validationResults;
    }

    public ValidationException(final String message, final Collection<ValidationResult> validationResults) {
        super(message);
        this.validationResults = validationResults;
    }

    @Override
    public Integer getStatusCode() {
        return 400;
    }

    @Override
    public Object getDetails() {
        return validationResults;
    }
}
