package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.ModelHolder;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator;

import javax.annotation.Nullable;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Named;

@Module
public class DefaultPayloadValidatorModule {
    public static final String PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION = "payloadValidatorRequiredStringValidatorOption";

    @JudoApplicationScope
    @Provides
    public PayloadValidator providesPayloadValidator(
            ModelHolder models,
            DataTypeManager dataTypeManager,
            ValidatorProvider validatorProvider,
            IdentifierProvider identifierProvider,
            @Named(PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION) @Nullable String requiredStringValidatorOption
    ) {
        return DefaultPayloadValidator.builder()
                .asmUtils(new AsmUtils(models.getAsmModel().getResourceSet()))
                .coercer(dataTypeManager.getCoercer())
                .identifierProvider(identifierProvider)
                .validatorProvider(validatorProvider)
                .requiredStringValidatorOption(
                        DefaultPayloadValidator.RequiredStringValidatorOption.valueOf(Objects.requireNonNullElse(requiredStringValidatorOption, "ACCEPT_NON_EMPTY")))
                .build();
    }

}
