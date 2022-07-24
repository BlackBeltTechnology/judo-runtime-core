package hu.blackbelt.judo.runtime.core.validator;


import java.util.Collection;
import java.util.Optional;

public interface ValidatorProvider {

    void addValidator(Validator validator);

    void removeValidator(Validator validator);

    void removeValidatorType(Class<? extends Validator> validatorType);

    void replaceValidator(Validator validator);

    <T extends Validator> Optional<Validator> getInstance(Class<T> clazz);

    Collection<Validator> getValidators();

}
