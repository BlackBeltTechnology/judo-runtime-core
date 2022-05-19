package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class GetMetadataCall<ID> implements BehaviourCall<ID> {

    final AsmUtils asmUtils;
    final Supplier<OpenIdConfigurationProvider> openIdConfigurationProvider;

    private static final String SECURITY_KEY = "security";

    private static final String ISSUER = "issuer";
    private static final String AUTH_ENDPOINT = "auth_endpoint";
    private static final String TOKEN_ENDPOINT = "token_endpoint";
    private static final String LOGOUT_ENDPOINT = "end_session_endpoint";

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_METADATA).isPresent();
    }

    @Override
    public Object call(final Map<String, Object> exchange, final EOperation operation) {
        final Payload result = Payload.empty();
        final List<Payload> securityList = new ArrayList<>();

        asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .filter(owner -> owner instanceof EClass)
                .map(owner -> (EClass) owner)
                .filter(owner -> AsmUtils.isActorType(owner))
                .ifPresent(actorType -> {
                    final Payload security = Payload.empty();
                    final Optional<String> realm = AsmUtils.getExtensionAnnotationValue(actorType, "realm", false);
                    if (realm.isPresent()) {
                        security.put("name", realm.get());
                        security.put("clientBaseUrl", "TODO");
                        Optional.ofNullable(openIdConfigurationProvider.get()).ifPresent(provider -> {
                            security.put("openIdConfigurationUrl", provider.getOpenIdConfigurationUrl(actorType));
                            security.put("clientId", provider.getClientId(actorType));
                            final Map<String, Object> openIdConfiguration = provider.getOpenIdConfiguration(actorType);
                            if (openIdConfiguration != null) {
                                security.put("issuer", openIdConfiguration.get(ISSUER));
                                security.put("authEndpoint", openIdConfiguration.get(AUTH_ENDPOINT));
                                security.put("tokenEndpoint", openIdConfiguration.get(TOKEN_ENDPOINT));
                                security.put("logoutEndpoint", openIdConfiguration.get(LOGOUT_ENDPOINT));
                            }
                            security.put("defaultScopes", "email");
                        });
                        securityList.add(security);
                    }
                });

        result.put(SECURITY_KEY, securityList);

        return result;
    }
}
