package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * JNG-6421: a pre-built {@link ModelRuntime} is installed on the fixture as-is — the fixture
 * MUST use the shared model loader, coercer and QueryFactory instead of building new ones.
 */
@DisplayName("JudoRuntimeFixture#applyModelRuntime installs the shared model runtime")
class ApplyModelRuntimeTest {

    @Test
    void installsModelLoaderCoercerAndQueryFactory() {
        JudoModelLoader loader = mock(JudoModelLoader.class);
        ExtendableCoercer coercer = mock(ExtendableCoercer.class);
        QueryFactory queryFactory = mock(QueryFactory.class);

        JudoRuntimeFixture fixture = new JudoRuntimeFixture();
        fixture.applyModelRuntime(new ModelRuntime(loader, coercer, queryFactory));

        assertSame(loader, fixture.modelHolder);
        assertSame(coercer, fixture.coercer);
        assertSame(queryFactory, fixture.queryFactory());
    }
}
