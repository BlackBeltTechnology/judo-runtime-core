package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultIdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;

public class DefaultIdentifierSignerProvider implements Provider<IdentifierSigner> {

    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";
    AsmModel asmModel;
    IdentifierProvider identifierProvider;
    DataTypeManager dataTypeManager;
    String secret;

    @Inject
    public DefaultIdentifierSignerProvider(AsmModel asmModel,
                                            IdentifierProvider identifierProvider,
                                            DataTypeManager dataTypeManager,
                                            @Named(IDENTIFIER_SIGNER_SECRET) String secret) {
        this.asmModel = asmModel;
        this.identifierProvider = identifierProvider;
        this.dataTypeManager = dataTypeManager;
        this.secret = secret;
    }

    @Override
    public IdentifierSigner get() {
        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(secret)
                .build();
    }
}
