package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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
