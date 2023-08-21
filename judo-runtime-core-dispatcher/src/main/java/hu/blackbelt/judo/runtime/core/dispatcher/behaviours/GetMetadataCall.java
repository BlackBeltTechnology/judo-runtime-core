package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import lombok.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class GetMetadataCall<ID> implements BehaviourCall<ID> {

    final AsmModel asmModel;
    final AsmUtils asmUtils;
    final Supplier<OpenIdConfigurationProvider> openIdConfigurationProvider;
    final OperationCallInterceptorProvider interceptorProvider;
    private static final String SECURITY_KEY = "security";

    private static final String ISSUER = "issuer";
    private static final String AUTH_ENDPOINT = "auth_endpoint";
    private static final String TOKEN_ENDPOINT = "token_endpoint";
    private static final String LOGOUT_ENDPOINT = "end_session_endpoint";

    public GetMetadataCall(AsmModel asmModel, Supplier<OpenIdConfigurationProvider> openIdConfigurationProvider, OperationCallInterceptorProvider interceptorProvider) {
        this.asmModel = asmModel;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.openIdConfigurationProvider = openIdConfigurationProvider;
        this.interceptorProvider = interceptorProvider;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_METADATA).isPresent();
    }

    @Override
    public Object call(final Map<String, Object> exchange, final EOperation operation) {
        CallInterceptorUtil<GetMetadataCallPayload, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                GetMetadataCallPayload.class, Payload.class, asmModel, operation, interceptorProvider
        );

        final List<Payload> securityList = new ArrayList<>();
        asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .filter(owner -> owner instanceof EClass)
                .map(owner -> (EClass) owner)
                .filter(AsmUtils::isActorType)
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

        GetMetadataCallPayload inputParameter = callInterceptorUtil.preCallInterceptors(GetMetadataCallPayload.builder()
                        .securityList(securityList)
                        .instance(Payload.asPayload(exchange))
                        .build());

        final Payload result = Payload.empty();
        if (callInterceptorUtil.isOriginalCalled()) {
            result.put(SECURITY_KEY, inputParameter.getSecurityList());
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class GetMetadataCallPayload {
        @NonNull
        Payload instance;

        @NonNull
        List<Payload> securityList;
    }

}
