package hu.blackbelt.judo.runtime.core.dispatcher;

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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleResolver;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcorePackage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.newEAttributeBuilder;
import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.newEClassBuilder;
import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.newEPackageBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Integration test for the login-time principal-locale refresh against a <b>real ASM model</b>.
 *
 * <p>Unlike {@code DefaultActorResolverLocaleRefreshTest} (which injects the {@code persistable}
 * boolean directly), this test builds a genuine ASM model with an entity and a mapped actor, so the
 * D3a introspection path — {@code AsmUtils.isMappedTransferObjectType}, {@code getMappedAttribute},
 * and the {@code transient} check — runs for real. The DAO is mocked to observe the write. This
 * exercises the exact glue that the unit tests deliberately stub, without needing a datasource,
 * Keycloak, or a generated model.
 */
class DefaultActorResolverLocaleRefreshModelTest {

    private static final String ID_KEY = "__id";
    private static final String LOCALE_ATTR = "locale";

    private DAO dao;
    private DefaultActorResolver resolver;
    private AsmUtils asmUtils;
    private EClass mappedActor;      // actor with a mapped, persistable locale attribute
    private EClass transientActor;   // actor whose locale attribute is transient (not persistable)

    private static IdentifierProvider identifierProvider() {
        return new IdentifierProvider() {
            @Override
            public Serializable get() {
                return null;
            }

            @Override
            public Class<? extends Serializable> getType() {
                return Serializable.class;
            }

            @Override
            public String getName() {
                return ID_KEY;
            }
        };
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        final EcorePackage ecore = EcorePackage.eINSTANCE;

        // --- entity: Test.User { username, locale } ---
        final EAttribute entityUsername = newEAttributeBuilder().withName("username").withEType(ecore.getEString()).build();
        final EAttribute entityLocale = newEAttributeBuilder().withName(LOCALE_ATTR).withEType(ecore.getEString()).build();
        final EClass userEntity = newEClassBuilder().withName("User")
                .withEStructuralFeatures(entityUsername, entityLocale).build();

        // --- mapped actor: Test.UserActor { locale (binding=locale) } ---
        final EAttribute actorLocale = newEAttributeBuilder().withName(LOCALE_ATTR).withEType(ecore.getEString()).build();
        final EClass userActor = newEClassBuilder().withName("UserActor")
                .withEStructuralFeatures(actorLocale).build();

        // --- actor whose locale is transient (not persistable) ---
        final EAttribute transientLocale = newEAttributeBuilder().withName(LOCALE_ATTR).withEType(ecore.getEString()).build();
        final EClass transientActorClass = newEClassBuilder().withName("TransientActor")
                .withEStructuralFeatures(transientLocale).build();

        final EPackage pkg = newEPackageBuilder().withName("Test").withNsPrefix("test")
                .withNsURI("http://blackbelt.hu/judo/test")
                .withEClassifiers(userEntity, userActor, transientActorClass).build();

        final AsmModel asmModel = AsmModel.buildAsmModel().uri(URI.createURI("asm:test")).build();
        asmModel.addContent(pkg);
        asmUtils = new AsmUtils(asmModel.getResourceSet());

        // entity annotations
        AsmUtils.addExtensionAnnotation(userEntity, "entity", "true");
        // mapped actor annotations
        final String userFq = AsmUtils.getClassifierFQName(userEntity);
        AsmUtils.addExtensionAnnotation(userActor, "mappedEntityType", userFq);
        AsmUtils.addExtensionAnnotation(actorLocale, "binding", LOCALE_ATTR);
        // transient-actor annotations: mapped to entity, but locale attribute marked transient
        AsmUtils.addExtensionAnnotation(transientActorClass, "mappedEntityType", userFq);
        AsmUtils.addExtensionAnnotation(transientLocale, "binding", LOCALE_ATTR);
        AsmUtils.addExtensionAnnotation(transientLocale, "transient", "true");

        this.mappedActor = userActor;
        this.transientActor = transientActorClass;

        this.dao = mock(DAO.class);
        this.resolver = DefaultActorResolver.builder()
                .dataTypeManager(new DataTypeManager(new DefaultCoercer()))
                .dao(dao)
                .asmModel(asmModel)
                .identifierProvider(identifierProvider())
                .localeConfig(hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig.builder()
                        .principalLocaleAttribute(LOCALE_ATTR)
                        .supportedLanguages("en-US,hu-HU")
                        .defaultLanguage("en-US")
                        .browserLanguageCheck(true)
                        .build())
                .build();
    }

    @Test
    @DisplayName("real model: claim locale differs from stored -> DAO.update persists resolved value")
    void claimDiffersFromStoredPersists() {
        final Payload actor = Payload.map(ID_KEY, "u1", LOCALE_ATTR, "en-US");
        final Map<String, Object> claims = new HashMap<>();
        claims.put(LOCALE_ATTR, "hu-HU"); // OIDC locale claim, mapped to the actor attribute name

        resolver.refreshActorLocale(mappedActor, actor, claims);

        final Payload expected = Payload.map(ID_KEY, "u1", LOCALE_ATTR, "hu-HU");
        verify(dao).update(eq(mappedActor), eq(expected), isNull());
        // in-request actor payload reflects the resolved value immediately
        assertThat((String) actor.get(LOCALE_ATTR), is(equalTo("hu-HU")));
    }

    @Test
    @DisplayName("real model: browser Accept-Language wins as top tier when enabled")
    void browserHintWins() {
        final Payload actor = Payload.map(ID_KEY, "u1", LOCALE_ATTR, "en-US");
        final Map<String, Object> claims = new HashMap<>();
        claims.put(LOCALE_ATTR, "en-US");
        claims.put(PrincipalLocaleResolver.ACCEPT_LANGUAGE_ATTRIBUTE, "hu-HU,en-US;q=0.7");

        resolver.refreshActorLocale(mappedActor, actor, claims);

        verify(dao).update(eq(mappedActor), eq(Payload.map(ID_KEY, "u1", LOCALE_ATTR, "hu-HU")), isNull());
    }

    @Test
    @DisplayName("real model: resolved equals stored -> no DAO.update")
    void resolvedEqualsStoredNoUpdate() {
        final Payload actor = Payload.map(ID_KEY, "u1", LOCALE_ATTR, "hu-HU");
        final Map<String, Object> claims = new HashMap<>();
        claims.put(LOCALE_ATTR, "hu-HU");

        resolver.refreshActorLocale(mappedActor, actor, claims);

        verify(dao, never()).update(any(), any(), any());
    }

    @Test
    @DisplayName("real model: transient locale attribute (D3a) -> not persistable, no DAO.update")
    void transientAttributeNotPersisted() {
        final Payload actor = Payload.map(ID_KEY, "u1", LOCALE_ATTR, "en-US");
        final Map<String, Object> claims = new HashMap<>();
        claims.put(LOCALE_ATTR, "hu-HU");

        resolver.refreshActorLocale(transientActor, actor, claims);

        verify(dao, never()).update(any(), any(), any());
    }

    @Test
    @DisplayName("sanity: the built model really is a mapped transfer object with a mapped locale attribute")
    void modelIntrospectionSanity() {
        assertThat(asmUtils.isMappedTransferObjectType(mappedActor), is(true));
        final EAttribute locale = (EAttribute) mappedActor.getEStructuralFeature(LOCALE_ATTR);
        assertThat(asmUtils.getMappedAttribute(locale).isPresent(), is(true));
        assertThat(AsmUtils.annotatedAsTrue(locale, "transient"), is(false));
    }
}
