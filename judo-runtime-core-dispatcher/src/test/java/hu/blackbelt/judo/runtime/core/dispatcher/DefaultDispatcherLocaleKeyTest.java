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
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.RequestLocaleHolder;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.PrincipalLocaleProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.runtime.core.validator.DefaultValidatorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.InternalServerException;
import hu.blackbelt.judo.runtime.core.security.LocaleResolutionLevel;
import hu.blackbelt.judo.runtime.core.security.PrincipalLocaleConfig;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcorePackage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.eclipse.emf.ecore.util.builder.EcoreBuilders.newEPackageBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks in the {@code LOCALE_KEY} propagation contract of
 * {@link DefaultDispatcher#callOperation(String, Map)}: the dispatcher SHALL populate
 * {@code Context[LOCALE_KEY]} <b>only when the exchange carries it</b>, and SHALL NOT synthesise a
 * value from a JVM default or a builder-provided fallback.
 *
 * <p>Guards the fix for the shadowing bug documented in
 * {@code docs/JNG-6415-dispatcher-locale-key-shadowing.md} — before the fix,
 * {@code callOperation} unconditionally wrote {@code Context[LOCALE_KEY]} with
 * {@code Locale.getDefault()} on empty exchanges, which shadowed the anonymous
 * {@link RequestLocaleHolder} browser tier consulted by {@link PrincipalLocaleProvider}.
 *
 * <p>Test approach: since {@code callOperation} throws
 * {@link InternalServerException} (wrapping {@link UnsupportedOperationException}) as soon as it
 * cannot find the requested operation in the ASM model — and that lookup happens <i>after</i> the
 * {@code LOCALE_KEY} propagation block — the tests invoke {@code callOperation} with a
 * nonexistent operation name and inspect {@link Context#getAs(Class, String)} after the throw.
 * The dispatcher's {@code finally} block does not clear the context (it only does so when the
 * operation was marked {@code __exposed}), so the state left by the propagation block survives.
 */
class DefaultDispatcherLocaleKeyTest {

    private static final String NONEXISTENT_OP = "no.such.Operation";

    private Context context;
    private DefaultDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        RequestLocaleHolder.clear();
        context = new ThreadContext(new DataTypeManager(new DefaultCoercer()));

        // Minimal AsmModel with an empty package -> operationCache lookup will return empty
        // and callOperation throws before reaching any real operation logic. That is fine:
        // we only care about state populated by the LOCALE_KEY block, which runs earlier.
        final AsmModel asmModel = AsmModel.buildAsmModel().uri(URI.createURI("asm:locale-key-test")).build();
        final EPackage pkg = newEPackageBuilder()
                .withName("Test").withNsPrefix("test").withNsURI("http://blackbelt.hu/judo/test-locale-key")
                .build();
        // seed with the built-in EcorePackage so AsmUtils setup doesn't choke on an empty resource
        asmModel.addContent(pkg);
        asmModel.getResourceSet().getPackageRegistry().put(EcorePackage.eNS_URI, EcorePackage.eINSTANCE);

        final DataTypeManager dtm = new DataTypeManager(new DefaultCoercer());

        // authenticateActor is void; default mock does nothing (perfect for anonymous path).
        final ActorResolver actorResolver = mock(ActorResolver.class);

        // Sdk / script maps must return non-null empty maps so setupBehaviourCalls does not NPE.
        final DispatcherFunctionProvider dispatcherFunctionProvider = mock(DispatcherFunctionProvider.class);
        when(dispatcherFunctionProvider.getSdkFunctions()).thenReturn(new HashMap<>());
        when(dispatcherFunctionProvider.getScriptFunctions()).thenReturn(new HashMap<>());

        // MetricsCollector.start(...) must return a non-null AutoCloseable.
        final MetricsCollector metricsCollector = mock(MetricsCollector.class);
        when(metricsCollector.start(anyString()))
                .thenAnswer(inv -> new MetricsCancelToken(inv.getArgument(0), metricsCollector));

        final IdentifierProvider identifierProvider = mock(IdentifierProvider.class);
        when(identifierProvider.getName()).thenReturn("__id");

        final DAO dao = mock(DAO.class);
        dispatcher = DefaultDispatcher.builder()
                .validatorProvider(new DefaultValidatorProvider(dao, identifierProvider, asmModel, context))
                .asmModel(asmModel)
                .expressionModel(ExpressionModel.buildExpressionModel()
                        .name("locale-key-test")
                        .resourceSet(ExpressionModelResourceSupport.createExpressionResourceSet())
                        .build())
                .dao(dao)
                .identifierProvider(identifierProvider)
                .dispatcherFunctionProvider(dispatcherFunctionProvider)
                .operationCallInterceptorProvider(mock(OperationCallInterceptorProvider.class))
                .dataTypeManager(dtm)
                .identifierSigner(mock(IdentifierSigner.class))
                .actorResolver(actorResolver)
                .context(context)
                .metricsCollector(metricsCollector)
                .payloadValidator(mock(PayloadValidator.class))
                .exporter(mock(Export.class))
                .accessManager(mock(AccessManager.class))
                .build();
    }

    @AfterEach
    void tearDown() {
        RequestLocaleHolder.clear();
        // Guard: never leave a LOCALE_KEY set for the next test.
        context.removeAll();
    }

    @Test
    @DisplayName("T1: exchange without LOCALE_KEY leaves Context[LOCALE_KEY] absent")
    void exchangeWithoutLocaleKeyLeavesContextAbsent() {
        final Map<String, Object> exchange = new HashMap<>();

        assertThrows(InternalServerException.class, () -> dispatcher.callOperation(NONEXISTENT_OP, exchange));

        // Post-fix: the LOCALE_KEY block does not write when the exchange lacks the key.
        // Pre-fix (bug): this would return Locale.getDefault().
        assertThat("Context[LOCALE_KEY] must not be synthesised from a JVM default fallback",
                context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY), is(nullValue()));
    }

    @Test
    @DisplayName("T2: exchange with LOCALE_KEY populates Context[LOCALE_KEY] with that value")
    void exchangeWithLocaleKeyPopulatesContext() {
        final Locale expected = Locale.forLanguageTag("hu-HU");
        final Map<String, Object> exchange = new HashMap<>();
        exchange.put(DefaultDispatcher.LOCALE_KEY, expected);

        assertThrows(InternalServerException.class, () -> dispatcher.callOperation(NONEXISTENT_OP, exchange));

        assertThat(context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY), is(equalTo(expected)));
    }

    @Test
    @DisplayName("T3: anonymous BROWSER-ceiling provider sees the holder locale through a dispatched op (empty exchange)")
    void anonymousBrowserTierReachableThroughDispatch() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .supportedLanguages("en-US,hu-HU,de-DE")
                .defaultLanguage("en-US")
                .localeResolutionLevel(LocaleResolutionLevel.BROWSER)
                .build();
        final PrincipalLocaleProvider provider = new PrincipalLocaleProvider(context, cfg);

        RequestLocaleHolder.set("hu-HU");
        final Map<String, Object> exchange = new HashMap<>();

        assertThrows(InternalServerException.class, () -> dispatcher.callOperation(NONEXISTENT_OP, exchange));

        final Optional<Locale> resolved = provider.getLocale();
        assertThat("Provider must consult the RequestLocaleHolder rather than a dispatcher-synthesised LOCALE_KEY",
                resolved.isPresent(), is(true));
        assertThat(resolved.get(), is(equalTo(Locale.forLanguageTag("hu-HU"))));
    }

    @Test
    @DisplayName("T4: explicit LOCALE_KEY in exchange still wins over the header (parent precedence preserved)")
    void explicitLocaleKeyWinsOverHeader() {
        final PrincipalLocaleConfig cfg = PrincipalLocaleConfig.builder()
                .supportedLanguages("en-US,hu-HU,de-DE")
                .defaultLanguage("en-US")
                .localeResolutionLevel(LocaleResolutionLevel.BROWSER)
                .build();
        final PrincipalLocaleProvider provider = new PrincipalLocaleProvider(context, cfg);

        RequestLocaleHolder.set("hu-HU");
        final Map<String, Object> exchange = new HashMap<>();
        exchange.put(DefaultDispatcher.LOCALE_KEY, Locale.forLanguageTag("en-US"));

        assertThrows(InternalServerException.class, () -> dispatcher.callOperation(NONEXISTENT_OP, exchange));

        final Optional<Locale> resolved = provider.getLocale();
        assertThat(resolved.isPresent(), is(true));
        assertThat("Explicit exchange LOCALE_KEY must win over the captured header",
                resolved.get(), is(equalTo(Locale.forLanguageTag("en-US"))));
        assertThat(context.getAs(Locale.class, DefaultDispatcher.LOCALE_KEY), is(notNullValue()));
    }
}
