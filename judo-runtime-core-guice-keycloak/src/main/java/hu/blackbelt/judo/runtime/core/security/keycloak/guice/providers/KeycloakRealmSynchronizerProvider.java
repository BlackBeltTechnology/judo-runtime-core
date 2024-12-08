package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakAdminClient;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakRealmSynchronizer;
import hu.blackbelt.judo.runtime.core.security.keycloak.KeycloakUserManager;
import hu.blackbelt.judo.tatami.core.TransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class KeycloakRealmSynchronizerProvider implements Provider<KeycloakRealmSynchronizer> {

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerSupportLoginByEmail")
    @Nullable
    boolean realmLoginByEmail = true;

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerClientAccessTypeForHuman")
    @Nullable
    String clientAccessTypeHuman = "CONFIDENTIAL";

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerClientAccessTypeForSystem")
    @Nullable
    String clientAccessTypeSystem = "BEARER_ONLY";

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerCorsAllowOrigin")
    @Nullable
    String corsAllowOrigin;

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerAsyncServiceCall")
    @Nullable
    Boolean asyncServiceCall = true;

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerRetryMaxAttempts")
    @Nullable
    Integer retryMaxAttempts = 1000;

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerRetryExponentialBackoff")
    @Nullable
    Boolean retryExponentialBackoff = true;

    @Inject(optional = true)
    @Named("KeycloakRealmSynchronizerRetryWaitDuration")
    @Nullable
    Long retryWaitDuration = 1000L;

    @Inject(optional = true)
    @Named("KeycloakIdentityManagerIsReady")
    @Nullable
    Consumer<KeycloakUserManager> identityManagerIsReady = (userManager) -> { userManager.setIdentityManagerReady(true); };

    @Inject
    KeycloakAdminClient keycloakAdminClient;

    @Inject
    KeycloakUserManager keycloakUserManager;

    @Inject
    KeycloakModel keycloakModel;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    JudoModelLoader judoModelLoader;

    public KeycloakRealmSynchronizer get() {
        Collection<String> corsAllowOrigin;
        if (this.corsAllowOrigin != null) {
            corsAllowOrigin = Arrays.stream(this.corsAllowOrigin.split(",")).map(v -> v.trim()).collect(Collectors.toList());
        } else {
            corsAllowOrigin = Collections.emptySet();
        }
        KeycloakRealmSynchronizer.AccessType systemDefaultAccessType = KeycloakRealmSynchronizer.AccessType.valueOf(clientAccessTypeSystem);
        KeycloakRealmSynchronizer.AccessType humanDefaultSystemAccessType = KeycloakRealmSynchronizer.AccessType.valueOf(clientAccessTypeHuman);
        AtomicReference<KeycloakRealmSynchronizer> keycloakRealmSynchronizerAtomicReference = new AtomicReference<>();
        KeycloakRealmSynchronizer keycloakRealmSynchronizer = KeycloakRealmSynchronizer.builder()
                .asyncServiceCall(asyncServiceCall)
                .corsAllowOrigin(corsAllowOrigin)
                .systemDefaultAccessType(systemDefaultAccessType)
                .humanDefaultSystemAccessType(humanDefaultSystemAccessType)
                .supportLoginByEmail(realmLoginByEmail)
                .retryMaxAttempts(retryMaxAttempts)
                .retryExponentialBackoff(retryExponentialBackoff)
                .retryWaitDuration(retryWaitDuration)
                .keycloakModel(keycloakModel)
                .keycloakAdminClient(keycloakAdminClient)
                .transformationTrace(judoModelLoader.getAsm2keycloak())
                .transformationTraceService(transformationTraceService)
                .registerIdentityManagerReady(() -> {
                    identityManagerIsReady.accept(keycloakUserManager);
                })
                .build();
        keycloakRealmSynchronizerAtomicReference.set(keycloakRealmSynchronizer);
        keycloakRealmSynchronizer.synchronizeAllRealms();
        return keycloakRealmSynchronizer;
    }
}
