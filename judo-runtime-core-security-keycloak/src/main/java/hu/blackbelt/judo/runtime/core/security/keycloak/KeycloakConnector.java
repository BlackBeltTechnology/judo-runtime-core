package hu.blackbelt.judo.runtime.core.security.keycloak;

/*-
 * #%L
 * JUDO Services Keycloak Security
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
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.jaxrs.client.WebClient;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import javax.ws.rs.core.Form;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class KeycloakConnector implements OpenIdConfigurationProvider {

    @Setter
    AsmModel asmModel;

    @Setter
    ObjectMapper objectMapper;

    @Getter
    private String serverUrl;

    @Getter
    private String externalUrl;

    private String adminUser;
    private String adminPassword;
    private String clientSecret;

    final List providers = new ArrayList();

    private EMap<EClass, String> realms = ECollections.asEMap(new ConcurrentHashMap<>());
    private EMap<EClass, Map> openIdConfigurations = ECollections.asEMap(new ConcurrentHashMap<>());


    @Builder
    public KeycloakConnector(
            @NonNull AsmModel asmModel,
            @NonNull ObjectMapper objectMapper,
            String serverUrl,
            String externalUrl,
            String adminUser,
            String adminPassword,
            String clientSecret) {
        this.asmModel = asmModel;
        this.objectMapper = objectMapper;
        this.serverUrl = serverUrl;
        this.externalUrl = externalUrl;
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
        this.clientSecret = clientSecret;
        providers.add(new JacksonJaxbJsonProvider(objectMapper, JacksonJaxbJsonProvider.DEFAULT_ANNOTATIONS));

        final AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        asmUtils.getAllActorTypes().forEach(actorType -> AsmUtils.getExtensionAnnotationValue(actorType, "realm", false)
                    .ifPresent(realm -> realms.put(actorType, realm)));
    }

    private String getAccessToken() {
        final Form form = new Form()
                .param("username", adminUser)
                .param("password", adminPassword)
                .param("grant_type", "password")
                .param("client_id", "admin-cli");
        if (clientSecret != null) {
            form.param("client_secret", clientSecret);
        }
        final Map response = WebClient.create(serverUrl, providers)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .path("/realms/master/protocol/openid-connect/token")
                .post(form, Map.class);

        return (String) response.get("access_token");
    }

    public WebClient getClient(final String realm) {
        return WebClient.create(serverUrl, providers)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .path("/realms/{realm}/.well-known/openid-configuration", realm);
    }

    public WebClient getAdminClient() {
        return WebClient.create(serverUrl, providers)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + getAccessToken())
                .path("/admin");
    }

    public static void logError(final Response response) {
        final Object fault = response.getEntity() instanceof InputStream ? new BufferedReader(new InputStreamReader(((InputStream) response.getEntity()), StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")) : response.getEntity();
        log.error("Operation failed: {}", fault);
    }

    @Override
    public String getOpenIdConfigurationUrl(final EClass actorType) {
        if (realms.containsKey(actorType)) {
            return getClient(realms.get(actorType)).getCurrentURI().toASCIIString();
        } else {
            log.warn("OpenID configuration not exists for invalid or anonymous actor");
            return null;
        }
    }

    @Override
    public Map<String, Object> getOpenIdConfiguration(final EClass actorType) {
        if (openIdConfigurations.containsKey(actorType)) {
            return openIdConfigurations.get(actorType);
        } else if (realms.containsKey(actorType)) {
            Map<String, Object> response = getClient(realms.get(actorType)).get(Map.class);
            if (externalUrl != null && !externalUrl.trim().isBlank()) {
                // Return public URL in configuration when defined
                response = response.entrySet().stream().collect(Collectors
                        .toMap(Map.Entry::getKey, e -> {
                            if (e.getValue() instanceof String) {
                                String val = (String) e.getValue();
                                if (val.startsWith(serverUrl)) {
                                    return val.replace(serverUrl, externalUrl);
                                }
                            }
                            return e.getValue();
                        }));
            }

            log.info("OpenID configuration for {}:\n{}", AsmUtils.getClassifierFQName(actorType), response);
            openIdConfigurations.put(actorType, response);
            return response;
        } else {
            log.warn("OpenID configuration not exists for invalid or anonymous actor");
            return null;
        }
    }

    @Override
    public void ping() {
        try {
            final Response response = WebClient.create(serverUrl, providers).path("/").get();
            checkState(response.getStatus() == 200, "Keycloak is not ready");
        } catch (RuntimeException ex) {
            throw new IllegalStateException("Keycloak is not available", ex);
        }
    }

    @Override
    public String getClientId(final EClass actorType) {
        return AsmUtils.isActorType(actorType) ? AsmUtils.getClassifierFQName(actorType).replace(".", "-") : null;
    }
}
