package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator;

import javax.annotation.Nullable;
import java.util.Objects;

public class DefaultPayloadValidatorProvider implements Provider<PayloadValidator> {
    public static final String PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION = "payloadValidatorRequiredStringValidatorOption";

    @Inject
    JudoModelLoader models;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    ValidatorProvider validatorProvider;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject(optional = true)
    @Named(PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION)
    @Nullable
    String requiredStringValidatorOption;


    public PayloadValidator get() {
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
