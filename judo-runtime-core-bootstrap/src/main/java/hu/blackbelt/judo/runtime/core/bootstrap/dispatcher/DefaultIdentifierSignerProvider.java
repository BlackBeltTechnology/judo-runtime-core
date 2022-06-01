package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultIdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;

import javax.annotation.Nullable;

@SuppressWarnings("rawtypes")
public class DefaultIdentifierSignerProvider implements Provider<IdentifierSigner> {

    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";

    @Inject
    AsmModel asmModel;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject(optional = true)
    @Named(IDENTIFIER_SIGNER_SECRET)
    @Nullable
    String secret;

    @Override
    @SuppressWarnings("unchecked")
    public IdentifierSigner get() {

        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(secret)
                .build();
    }
}
