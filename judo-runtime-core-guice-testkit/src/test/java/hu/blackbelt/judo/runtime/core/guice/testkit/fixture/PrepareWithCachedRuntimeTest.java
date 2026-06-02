package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of {@link JudoRuntimeFixture#prepareWithCachedRuntime}.
 *
 * <p>Asserts the contract on which the {@code BY_CLASS} / {@code SINGLETON}
 * runtime cache depends:
 * <ul>
 *   <li>every cached field of {@link CachedRuntime} is installed on the fixture;</li>
 *   <li>{@code injector.injectMembers(testInstance)} is invoked when {@code injectModulesTo} is non-null;</li>
 *   <li>no member-injection happens when {@code injectModulesTo} is null;</li>
 *   <li>passing a {@code null} cached runtime fails fast.</li>
 * </ul>
 *
 * <p>This is a pure unit test \u2014 no JUDO model, no datasource, no Liquibase. It
 * only exercises the field-wiring fast path that the cache invokes for every
 * test method after the cold path.
 */
@DisplayName("JudoRuntimeFixture#prepareWithCachedRuntime: field installation + member injection")
class PrepareWithCachedRuntimeTest {

    /** Tiny holder class used to verify {@code Injector#injectMembers} actually fires. */
    static class InjectionTarget {
        @Inject String greeting;
    }

    @Test
    void installsAllCachedFields() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override protected void configure() {
                bind(String.class).toInstance("hello-from-cache");
            }
        });

        CachedRuntime cached = new CachedRuntime(
                /* modelLoader        */ null,
                /* dialect            */ null,
                /* queryFactory       */ null,
                /* coercer            */ null,
                /* databaseModule     */ null,
                /* liquibaseExecutor  */ new CountingLiquibaseExecutor(),
                /* injector           */ injector,
                /* transactionManager */ null);

        JudoRuntimeFixture fixture = new JudoRuntimeFixture();
        fixture.prepareWithCachedRuntime(cached, /* no member-injection */ null);

        assertSame(injector, fixture.getInjector(),
                "Injector field must be the cached one");
        assertSame(cached.liquibaseExecutor, fixture.simpleLiquibaseExecutor,
                "Liquibase executor field must be the cached one");
        // Package-private accessors:
        assertSame(cached.queryFactory, fixture.queryFactory());
        assertSame(cached.transactionManager, fixture.transactionManager());
    }

    @Test
    void runsInjectMembersWhenTargetIsProvided() {
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override protected void configure() {
                bind(String.class).toInstance("injected!");
            }
        });
        CachedRuntime cached = new CachedRuntime(null, null, null, null, null, null, injector, null);

        InjectionTarget target = new InjectionTarget();
        assertNull(target.greeting, "field must be null before injection");

        new JudoRuntimeFixture().prepareWithCachedRuntime(cached, target);

        assertEquals("injected!", target.greeting,
                "prepareWithCachedRuntime must invoke injector.injectMembers(testInstance)");
    }

    @Test
    void doesNotInjectMembersWhenTargetIsNull() {
        Injector injector = Guice.createInjector(new AbstractModule() {});
        CachedRuntime cached = new CachedRuntime(null, null, null, null, null, null, injector, null);

        // Must not throw: the target is null, so nothing is injected.
        new JudoRuntimeFixture().prepareWithCachedRuntime(cached, null);
    }

    @Test
    void rejectsNullCachedRuntime() {
        JudoRuntimeFixture fixture = new JudoRuntimeFixture();
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> fixture.prepareWithCachedRuntime(null, null));
        assertTrue(ex.getMessage().toLowerCase().contains("must not be null"));
    }

    /**
     * Regression for the CodeRabbit major finding: {@code prepareWithCachedRuntime} previously
     * left {@code interceptorProvider} unset, so any cached-path test calling the public
     * {@link JudoRuntimeFixture#getInterceptorProvider()} hit a misleading
     * "Interceptor provider not available. Call init() first." error — even though the
     * extension itself chose the cached path, not the user.
     */
    @Test
    @DisplayName("Restores interceptorProvider so getInterceptorProvider() works on cached path")
    void restoresInterceptorProviderOnCachedPath() {
        Injector injector = Guice.createInjector(new AbstractModule() {});
        TestOperationCallInterceptorProvider provider = new TestOperationCallInterceptorProvider();

        CachedRuntime cached = new CachedRuntime(
                null, null, null, null, null, null, injector, null, provider);

        JudoRuntimeFixture fixture = new JudoRuntimeFixture();
        fixture.prepareWithCachedRuntime(cached, /* no member-injection */ null);

        assertSame(provider, fixture.getInterceptorProvider(),
                "interceptor provider on cached path MUST be the cached one, not a fresh instance");
    }

    /**
     * Backwards-compat: the 8-arg constructor (used by older tests) still produces a runtime
     * that {@code prepareWithCachedRuntime} accepts — the resulting fixture's
     * {@code interceptorProvider} is simply {@code null} and behaves as it did before this fix
     * for that legacy call site.
     */
    @Test
    void eightArgConstructorYieldsNullInterceptorProvider() {
        Injector injector = Guice.createInjector(new AbstractModule() {});
        CachedRuntime cached = new CachedRuntime(null, null, null, null, null, null, injector, null);
        assertNull(cached.interceptorProvider,
                "the backwards-compat 8-arg constructor MUST leave interceptorProvider null");
    }
}
