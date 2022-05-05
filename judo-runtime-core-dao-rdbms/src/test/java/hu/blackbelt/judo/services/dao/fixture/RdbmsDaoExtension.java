package hu.blackbelt.judo.services.dao.fixture;

import hu.blackbelt.judo.services.dispatcher.DefaultMetricsCollector;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RdbmsDaoExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {

    private Map<String, RdbmsDaoFixture> fixtureMap = new ConcurrentHashMap<>();

    private DefaultMetricsCollector metricsCollector = new DefaultMetricsCollector();
    private boolean enabledMetrics = Boolean.parseBoolean(System.getProperty("enabledMetrics", "false"));

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(RdbmsDaoFixture.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        metricsCollector.setEnabled(enabledMetrics);
        Optional<ExtensionContext> ctx = Optional.of(extensionContext);
        RdbmsDaoFixture fixture = null;
        String name = "";
        while (fixture == null && ctx.isPresent()) {
            if (ctx.get().getParent().isPresent()) {
                name = ctx.get().getDisplayName().replaceAll(" ", "") + (name.equals("") ? "" : ".") + name;
            }
            fixture = fixtureMap.get(ctx.get().getDisplayName());
            ctx = ctx.get().getParent();
        }
        if (fixture == null) {
            fixture = new RdbmsDaoFixture(sanitizeFileName(name), metricsCollector);
            fixtureMap.put(extensionContext.getDisplayName(), fixture);
        }
        return fixture;
    }

    private String sanitizeFileName(String name) {
        String result = name.replaceAll("\\(.*\\)", "");
        result = result.replaceAll("[^\\w-\\[\\]\\.]", "_");
        return result;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info("Running test: {}", context.getTestClass().map(c -> c.getSimpleName()).orElse("") + context.getTestMethod().map(m -> "#" + m.getName()).orElse(""));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Optional<ExtensionContext> ctx = Optional.of(context);
        if (ctx.isPresent()) {
            RdbmsDaoFixture fixture = fixtureMap.get(context.getDisplayName());
            if (fixture != null) {
                fixtureMap.remove(context.getDisplayName());
            }
        }

        log.info("Completed test: {}", context.getTestClass().map(c -> c.getSimpleName()).orElse("") + context.getTestMethod().map(m -> "#" + m.getName()).orElse(""));
    }
}
