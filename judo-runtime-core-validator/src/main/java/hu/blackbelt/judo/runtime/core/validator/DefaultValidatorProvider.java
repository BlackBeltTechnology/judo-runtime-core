package hu.blackbelt.judo.runtime.core.validator;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

public class DefaultValidatorProvider implements ValidatorProvider {

    Collection<Validator> validators = new CopyOnWriteArrayList<>(Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator()));

    public void addValidator(Validator validator) {
        validators.add(validator);
    }

    public void removeValidator(Validator validator) {
        validators.remove(validator);
    }

    public Collection<Validator> getValidators() {
        return validators;
    }
}
