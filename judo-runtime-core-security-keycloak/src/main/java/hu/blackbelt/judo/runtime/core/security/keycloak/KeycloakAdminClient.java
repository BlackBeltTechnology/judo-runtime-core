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


import hu.blackbelt.judo.meta.keycloak.Client;
import hu.blackbelt.judo.meta.keycloak.Realm;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cxf.jaxrs.client.WebClient;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakConnector.logError;

@Slf4j
@Builder
public class KeycloakAdminClient {

    @NonNull
    KeycloakConnector keycloakConnector;

    public List<Map<String, Object>> getUsersOfRealm(final String realm) {
        final List<Map<String, Object>> response = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .path("/realms/{realm}/users", realm)
                .get(List.class);

        return response;
    }

    public Optional<Map<String, Object>> getUserOfRealm(final String realm, final String username) {
        checkArgument(username != null, "Username must not be null");

        final List<Map<String, Object>> response = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .path("/realms/{realm}/users", realm)
                .query("username", username)
                .get(List.class);
        return response.stream().findAny();
    }

    public List<Client> getClientsOfRealm(final String realm) {
        final List<Map<String, Object>> response = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .path("/realms/{realm}/clients", realm)
                .get(List.class);
        return response.stream().map(c -> KeycloakObjectMapper.objectMapper().convertValue(c, Client.class)).collect(Collectors.toList());
    }

    public List<Realm> getListOfRealms() {
        final List<Map<String, Object>> response = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                .path("/realms")
                .get(List.class);
        return response.stream().map(r -> KeycloakObjectMapper.objectMapper().convertValue(r, Realm.class)).collect(Collectors.toList());
    }

    public void createOrUpdateClient(final String realm, final Client client, final boolean update) {
        log.info("{} client {} in realm: {}", new Object[]{(update ? "Updating" : "Creating"), client.getName(), realm});
        final WebClient webClient = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON);
        final Response response = update
                ? webClient.path("/realms/{realm}/clients/{client}", realm, client.getId()).put(client)
                : webClient.path("/realms/{realm}/clients", realm).post(client);
        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            logError(response);
            throw new IllegalStateException("Failed to create/update client");
        }
    }

    public void createOrUpdateRealm(final Realm realm, final boolean update) {
        log.info("{} realm: {}", (update ? "Updating" : "Creating"), realm.getRealm());
        final WebClient webClient = keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON);
        final Response response = update
                ? webClient.path("/realms/{realm}", realm.getRealm()).put(realm)
                : webClient.path("/realms").post(realm);
        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            logError(response);
            throw new IllegalStateException("Failed to create/update realm");
        }
    }

    public Supplier<Response> createUserRestCall(String realmName, Map<String, Object> payload) {
        return () -> keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .path("/realms/{realm}/users", realmName)
                .post(payload);
    }

    public Supplier<Response> updateUserRestCall(String realmName, String id, Map<String, Object> payload) {
        return () -> keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .path("/realms/{realm}/users/{userId}", realmName, id)
                .put(payload);
    }

    public Supplier<Response> deleteUserRestCall(String realmName, String id) {
        return () -> keycloakConnector.getAdminClient()
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON)
                .path("/realms/{realm}/users/{userId}", realmName, id).delete();
    }

}
