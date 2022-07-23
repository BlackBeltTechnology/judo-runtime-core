package hu.blackbelt.judo.runtime.core.validator;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import lombok.Builder;
import lombok.NonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class DefaultValidatorProvider<ID> implements ValidatorProvider {

    final DAO dao;

    final IdentifierProvider<ID> identifierProvider;

    final Context context;

    final Collection<Validator> validators;

    public DefaultValidatorProvider() {
        this(null, null, null);
    }

    @Builder
    public DefaultValidatorProvider(DAO dao, IdentifierProvider identifierProvider, Context context) {
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.context = context;
        validators = new CopyOnWriteArrayList<>(Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator()));
        if (dao != null && identifierProvider != null && context != null) {
            validators.add(new RangeValidator<ID>(dao, identifierProvider, context));
        }
    }

    public void addValidator(Validator validator) {
        validators.add(validator);
    }

    public void removeValidator(Validator validator) {
        validators.remove(validator);
    }

    @Override
    public void removeValidatorType(Class<? extends Validator> validatorType) {
        Collection<Validator> validatorsToRemove = validators.stream().filter(v -> validatorType.isAssignableFrom(v.getClass())).collect(Collectors.toList());
        validators.removeAll(validatorsToRemove);
    }

    @Override
    public void replaceValidator(Validator validator) {
        removeValidatorType(validator.getClass());
        validators.add(validator);
    }

    @Override
    public <T extends Validator> Optional<Validator> getInstance(Class<T> clazz) {
        return validators.stream().filter(v -> clazz.isAssignableFrom(v.getClass())).findFirst();
    }

    public Collection<Validator> getValidators() {
        return validators;
    }
}
