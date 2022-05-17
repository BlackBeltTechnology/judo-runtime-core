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

    @Inject(optional = true)
    @Nullable
    String secret;

    @Inject(optional = true)
    @Named(IDENTIFIER_SIGNER_SECRET)
    @Nullable
    IdentifierProvider identifierProvider;

    @Override
    @SuppressWarnings("unchecked")
    public IdentifierSigner get() {
        IdentifierProvider idprov = identifierProvider;
        if (idprov == null) {
            idprov = new UUIDIdentifierProvider();
        }

        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(idprov)
                .dataTypeManager(dataTypeManager)
                .secret(secret)
                .build();
    }
}
