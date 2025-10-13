package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JudoRuntimeByClassExtension
    implements
        ParameterResolver,
        BeforeEachCallback,
        AfterEachCallback,
        BeforeAllCallback {

    private static final Logger log = LoggerFactory.getLogger(
        JudoRuntimeByClassExtension.class
    );

    private JudoRuntimeFixture judoRuntimeFixture;

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext
            .getParameter()
            .getType()
            .isAssignableFrom(JudoRuntimeFixture.class);
    }

    @Override
    public Object resolveParameter(
        ParameterContext parameterContext,
        ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return judoRuntimeFixture;
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        judoRuntimeFixture = new JudoRuntimeFixture();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info(
            "Running test: {}",
            context
                    .getTestClass()
                    .map(c -> c.getSimpleName())
                    .orElse("") +
                context
                    .getTestMethod()
                    .map(m -> "#" + m.getName())
                    .orElse("")
        );
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        log.info(
            "Completed test: {}",
            context
                    .getTestClass()
                    .map(c -> c.getSimpleName())
                    .orElse("") +
                context
                    .getTestMethod()
                    .map(m -> "#" + m.getName())
                    .orElse("")
        );
    }
}
