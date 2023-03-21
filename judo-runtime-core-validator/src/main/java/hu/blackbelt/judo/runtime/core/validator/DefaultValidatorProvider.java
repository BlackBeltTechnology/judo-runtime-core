package hu.blackbelt.judo.runtime.core.validator;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
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

    final AsmModel asmModel;

    public DefaultValidatorProvider() {
        this(null, null, null, null);
    }

    @Builder
    public DefaultValidatorProvider(DAO dao, IdentifierProvider identifierProvider, AsmModel asmModel, Context context) {
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.context = context;
        this.asmModel = asmModel;
        validators = new CopyOnWriteArrayList<>(Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator()));
        if (dao != null && identifierProvider != null && context != null) {
            validators.add(new RangeValidator<ID>(dao, identifierProvider, context));
            if (asmModel != null) {
                validators.add(new UniqueAttributeValidator<>(dao, asmModel, identifierProvider, context));
            }
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
