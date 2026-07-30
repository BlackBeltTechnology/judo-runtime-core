package hu.blackbelt.judo.runtime.core.security.keycloak.cxf;

/*-
 * #%L
 * JUDO Services Keycloak Security for CXF
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import com.fasterxml.jackson.databind.ObjectMapper;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.keycloak.AttributeBinding;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.runtime.core.exception.AuthenticationRequiredException;
import hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleResolver;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.interceptor.security.AuthenticationException;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.security.SecurityContext;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.adapters.rotation.AdapterTokenVerifier;
import org.keycloak.common.VerificationException;
import org.keycloak.exceptions.TokenNotActiveException;
import org.keycloak.jose.jws.JWSInput;
import org.keycloak.jose.jws.JWSInputException;
import org.keycloak.representations.AccessToken;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.osgi.service.component.annotations.*;
import org.slf4j.MDC;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class KeycloakLoginInterceptor extends AbstractPhaseInterceptor<Message> {

    private static final String AUTHORIZATION = "Authorization";
    private static final String BEARER = "Bearer";
    private static final String ACCEPT_LANGUAGE = "Accept-Language";

    RealmExtractor realmExtractor;

    AsmModel asmModel;

    OpenIdConfigurationProvider openIdConfigurationProvider;

    TransformationTraceService transformationTraceService;

    private String authServerUrl;

    private Map<String, KeycloakDeployment> keycloakDeploymentMap = new ConcurrentHashMap<>();

    private final ObjectMapper tokenObjectMapper = new ObjectMapper();

    public KeycloakLoginInterceptor() {
        super(Phase.UNMARSHAL);
    }

    private AsmUtils asmUtils;


    private Map<String, String> clientToActorMap;

    /**
     * Whether to capture the incoming {@code Accept-Language} header into principal attributes so
     * the login-time locale resolver can use it as the top precedence tier. Derived once from the
     * builder's {@link LocaleResolutionLevel} — the enum belongs at the configuration boundary,
     * but the interceptor's runtime question is still "capture or not?", a boolean.
     */
    private boolean captureBrowserLanguage;

    @Builder
    public KeycloakLoginInterceptor(
            @NonNull RealmExtractor realmExtractor,
            @NonNull AsmModel asmModel,
            @NonNull OpenIdConfigurationProvider openIdConfigurationProvider,
            @NonNull TransformationTraceService transformationTraceService,
            Map<String, String> clientToActorMap,
            LocaleResolutionLevel localeResolutionLevel) {
        super(Phase.UNMARSHAL);
        this.realmExtractor = realmExtractor;
        this.asmModel = asmModel;
        this.openIdConfigurationProvider = openIdConfigurationProvider;
        this.transformationTraceService = transformationTraceService;
        this.authServerUrl = openIdConfigurationProvider.getServerUrl();
        this.clientToActorMap = clientToActorMap;
        // Default-on: null level means the deployment hasn't configured the ceiling, so honour
        // the LocaleResolutionLevel.DEFAULT (BROWSER) and capture.
        this.captureBrowserLanguage = localeResolutionLevel == null
                || localeResolutionLevel.includes(LocaleResolutionLevel.BROWSER);
        asmUtils = new AsmUtils(asmModel.getResourceSet());
        keycloakDeploymentMap.clear();
    }

    private synchronized KeycloakDeployment getKeycloakDeployment(final String realm, final String resource) {
        final KeycloakDeployment keycloakDeployment;
        if (keycloakDeploymentMap.containsKey(realm)) {
            keycloakDeployment = keycloakDeploymentMap.get(realm);
        } else {
            final AdapterConfig adapterConfig = new AdapterConfig();
            adapterConfig.setConnectionPoolSize(10);
            adapterConfig.setAuthServerUrl(authServerUrl);
            adapterConfig.setRealm(realm);
            adapterConfig.setResource(resource);
            adapterConfig.setSslRequired("external");
            adapterConfig.setPublicClient(true);
            adapterConfig.setConfidentialPort(0);
            keycloakDeployment = KeycloakDeploymentBuilder.build(adapterConfig);
            keycloakDeploymentMap.put(realm, keycloakDeployment);
        }

        return keycloakDeployment;
    }

    public void handleMessage(final Message message) {
        HttpServletRequest request = (HttpServletRequest) message.get("HTTP.REQUEST");
        final Optional<EClass> actorType = realmExtractor.extractActorType(request);

        final Optional<String> realm = actorType.isPresent() ? actorType.map(a -> AsmUtils.getExtensionAnnotationValue(a, "realm", false).orElse(null)) : Optional.empty();
        if (realm.isPresent()) {
            final Optional<String> bearerToken = extractBearerToken(request);
            if (bearerToken != null && bearerToken.isPresent()) {
                final KeycloakDeployment keycloakDeployment = getKeycloakDeployment(realm.get(), AsmUtils.getClassifierFQName(actorType.get()));
                JWSInput jws = null;
                try {
                    jws = new JWSInput(bearerToken.get());
                    final AccessToken accessToken = AdapterTokenVerifier.verifyToken(bearerToken.get(), keycloakDeployment);
                    final Map<String, Object> token = tokenObjectMapper.readValue(jws.getContent(), Map.class);

                    // map token (Keycloak) data to actor attributes
                    final EMap<String, EAttribute> mapping = ECollections.asEMap(actorType.get().getEAllAttributes().stream()
                            .collect(Collectors.toMap(claim -> ((AttributeBinding) transformationTraceService.getDescendantOfInstanceByModelType(asmModel.getName(), KeycloakModel.class, claim).get(0)).getAttributeName(), claim -> claim)));
                    // username is added to token as preferred_username by Keycloak
                    final EAttribute usernameClaim = mapping.get("username");
                    if (usernameClaim != null && "USERNAME".equals(AsmUtils.getExtensionAnnotationValue(usernameClaim, "claim", true).orElse("-"))) {
                        mapping.put("preferred_username", usernameClaim);
                    }
                    final Map<String, Object> attributes = token.entrySet().stream()
                            .collect(Collectors.toMap(e -> mapping.containsKey(e.getKey()) ? mapping.get(e.getKey()).getName() : e.getKey(), e -> e.getValue()));

                    // Capture the request Accept-Language header (gated) so the login-time locale
                    // resolver can use it as the top precedence tier.
                    captureAcceptLanguage(attributes, request, captureBrowserLanguage);

                    final String resolvedClient = resolveClient(accessToken.getIssuedFor());

                    MDC.put("user", accessToken.getPreferredUsername());
                    message.put(SecurityContext.class, KeycloakSecurityContext.builder()
                            .userPrincipal(JudoPrincipal.builder()
                                    .name(accessToken.getPreferredUsername())
                                    .realm(realm.get())
                                    .client(resolvedClient)
                                    .attributes(Collections.unmodifiableMap(attributes))
                                    .build())
                            .build());
                } catch (TokenNotActiveException ex) {
                    log.info("Authentication failed: {}", ex.getMessage());
                    if (log.isDebugEnabled()) {
                        log.debug("Bearer token: {}", bearerToken.get());
                        if (jws != null) {
                            log.debug("Decoded access token: {}", jws.readContentAsString());
                        }
                    }

                    throw new AuthenticationRequiredException(ValidationResult.builder()
                            .code("ACCESS_TOKEN_EXPIRED")
                            .level(ValidationResult.Level.ERROR)
                            .build());
                } catch (VerificationException | JWSInputException | IOException | RuntimeException ex) {
                    log.error("Authentication failed", ex);
                    throw new AuthenticationException("Authentication failed");
                }
            }
        }
    }

    private static Optional<String> extractBearerToken(final HttpServletRequest httpServletRequest) {
        final String authorization = httpServletRequest.getHeader(AUTHORIZATION);
        if (authorization != null) {
            if (authorization.toLowerCase().startsWith(BEARER.toLowerCase() + " ")) {
                return Optional.of(authorization.substring(BEARER.length() + 1));
            } else {
                log.warn("Authorization is not a " + BEARER + " token");
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private String resolveClient(final String clientName) {
        final String normalized = convertClientToActorName(clientName);
        if (clientToActorMap != null && normalized != null && clientToActorMap.containsKey(normalized)) {
            return clientToActorMap.get(normalized);
        }
        return normalized;
    }

    private static String convertClientToActorName(final String clientName) {
        return clientName != null ? clientName.replaceAll("-", ".").trim() : null;
    }

    /**
     * Stash the raw {@code Accept-Language} header into the principal attributes under
     * {@link PrincipalLocaleResolver#ACCEPT_LANGUAGE_ATTRIBUTE} when the browser-language gate is on
     * and a non-blank header is present. No-op otherwise (gate off, null request, missing/blank
     * header). Package-visible for unit testing.
     */
    static void captureAcceptLanguage(final Map<String, Object> attributes,
                                      final HttpServletRequest request,
                                      final boolean browserLanguageCheck) {
        if (!browserLanguageCheck || request == null) {
            return;
        }
        final String header = request.getHeader(ACCEPT_LANGUAGE);
        if (header != null && !header.trim().isEmpty()) {
            attributes.put(PrincipalLocaleResolver.ACCEPT_LANGUAGE_ATTRIBUTE, header);
        }
    }
}
