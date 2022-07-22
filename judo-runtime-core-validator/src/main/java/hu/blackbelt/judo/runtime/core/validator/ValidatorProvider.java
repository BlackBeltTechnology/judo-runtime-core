package hu.blackbelt.judo.runtime.core.validator;

import java.util.Arrays;
import java.util.Collection;

public interface ValidatorProvider {

    void addValidator(Validator validator);
    void removeValidator(Validator validator);

    default Collection<Validator> getValidators() {
        return Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator());
    }
}
