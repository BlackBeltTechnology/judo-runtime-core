package hu.blackbelt.judo.runtime.core.validator;

import hu.blackbelt.judo.runtime.core.validator.*;

import java.util.Arrays;
import java.util.Collection;

public interface ValidatorProvider {

    default Collection<Validator> getValidators() {
        return Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator());
    }
}
