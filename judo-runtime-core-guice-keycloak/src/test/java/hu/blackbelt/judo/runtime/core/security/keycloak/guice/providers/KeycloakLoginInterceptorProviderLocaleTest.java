package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;
import hu.blackbelt.judo.runtime.core.security.keycloak.cxf.KeycloakLoginInterceptor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the Guice–Keycloak wiring gap that {@code replace-browser-check-with-resolution-level}
 * closes (design D7): {@link KeycloakLoginInterceptorProvider} now threads the
 * {@link PrincipalLocaleConfig#getLocaleResolutionLevel() localeResolutionLevel} into the built
 * {@link KeycloakLoginInterceptor}, so a Guice deployment that opts into {@code PRINCIPAL} or
 * {@code IDENTITY_PROVIDER} actually sees the interceptor's browser-language capture turn off.
 *
 * <p>Before this fix the provider never set the toggle at all, silently defaulting to browser-on
 * regardless of what the deployment configured. See
 * {@code openspec/changes/replace-browser-check-with-resolution-level/design.md} §D7 and the
 * MODIFIED "Guice and Spring wiring" requirement in the change's spec delta.
 */
class KeycloakLoginInterceptorProviderLocaleTest {

    /**
     * Introspects the private {@code captureBrowserLanguage} field on the constructed
     * {@link KeycloakLoginInterceptor}. Reflection here is deliberate: the interceptor deep-holds
     * the derived boolean and does not expose it (D6 keeps the enum at the configuration
     * boundary), so reading it is the surgical way to prove the provider threaded the level
     * correctly without spinning up the full CXF machinery.
     */
    private static boolean readCaptureFlag(final KeycloakLoginInterceptor interceptor) throws Exception {
        final Field field = KeycloakLoginInterceptor.class.getDeclaredField("captureBrowserLanguage");
        field.setAccessible(true);
        return field.getBoolean(interceptor);
    }

    private static KeycloakLoginInterceptorProvider providerWith(final PrincipalLocaleConfig localeConfig) {
        final KeycloakLoginInterceptorProvider provider = new KeycloakLoginInterceptorProvider();
        // Mocks for the required @Inject collaborators — nothing we assert on touches these.
        final AsmModel asmModel = mock(AsmModel.class);
        final ResourceSet resourceSet = new ResourceSetImpl();
        when(asmModel.getResourceSet()).thenReturn(resourceSet);
        final JudoModelLoader models = mock(JudoModelLoader.class);
        when(models.getAsmModel()).thenReturn(asmModel);
        provider.models = models;
        provider.openIdConfigurationProvider = mock(OpenIdConfigurationProvider.class);
        provider.realmExtractor = mock(RealmExtractor.class);
        provider.transformationTraceService = mock(TransformationTraceService.class);
        provider.localeConfig = localeConfig;
        return provider;
    }

    @Test
    void principalCeilingDisablesBrowserCapture() throws Exception {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .localeResolutionLevel(LocaleResolutionLevel.PRINCIPAL)
                .build();
        assertThat(readCaptureFlag(providerWith(cfg).get()), is(false));
    }

    @Test
    void identityProviderCeilingDisablesBrowserCapture() throws Exception {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .localeResolutionLevel(LocaleResolutionLevel.IDENTITY_PROVIDER)
                .build();
        assertThat(readCaptureFlag(providerWith(cfg).get()), is(false));
    }

    @Test
    void browserCeilingEnablesBrowserCapture() throws Exception {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .localeResolutionLevel(LocaleResolutionLevel.BROWSER)
                .build();
        assertThat(readCaptureFlag(providerWith(cfg).get()), is(true));
    }

    @Test
    void missingLocaleConfigDefaultsToBrowserCapture() throws Exception {
        // Simulates the "no PrincipalLocaleConfig binding" case: provider stays with null,
        // interceptor's own null-tolerance treats it as LocaleResolutionLevel.DEFAULT (BROWSER),
        // so capture is on — matches the pre-refactor default-on behaviour.
        assertThat(readCaptureFlag(providerWith(null).get()), is(true));
    }
}
