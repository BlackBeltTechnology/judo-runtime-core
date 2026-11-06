package hu.blackbelt.judo.runtime.core.spring;

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

import ch.qos.logback.classic.Level;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.query.QueryModelLoadOrBuild;
import hu.blackbelt.judo.runtime.core.spring.ModelSources.State;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.MapPropertySource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tasks 1.6, 2.4 (TP-24): the Spring context of the runtime configuration restores the {@link QueryFactory} from the
 * persisted query model on the classpath, in every mode and for every state of the artifact, exposes exactly one
 * {@code ExpressionModel} and an optional {@link PersistedQueryModel}, and an application with its own model beans builds.
 * <p>
 * The context is the production configuration ({@link JudoModelLoaderConfiguration}, {@link JudoModelConfiguration},
 * {@link JudoDefaultSpringConfiguration}); its collaborators outside the models are stubs, and only the
 * {@code QueryFactory} bean is created eagerly (everything else is lazy), as the decision is the subject here.
 */
class QueryModelSpringContextTest {

    private File modelDirectory;

    @AfterEach
    void removeModelsFromClasspath() throws IOException {
        if (modelDirectory != null && modelDirectory.exists()) {
            try (Stream<java.nio.file.Path> paths = Files.walk(modelDirectory.toPath())) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /** Put the model files on the test classpath: {@code target/test-classes/model}. */
    private void putOnClasspath(final State state) throws Exception {
        final File testClasses = new File(NorthwindModelSet.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        modelDirectory = new File(testClasses, "model");
        removeModelsFromClasspath();
        ModelSources.write(NorthwindModelSet.shared(), modelDirectory, state);
    }

    /** Collaborators of {@link JudoDefaultSpringConfiguration} that are not the subject: stubs. */
    private static void registerStubs(final AnnotationConfigApplicationContext context) {
        final List<Class<?>> models = Arrays.asList(AsmModel.class, Asm2RdbmsTransformationTrace.class, RdbmsModel.class,
                ExpressionModel.class, MeasureModel.class, JudoModelLoader.class, PersistedQueryModel.class);
        final List<Class<?>> defined = new ArrayList<>();
        for (Method method : JudoDefaultSpringConfiguration.class.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Bean.class)) {
                defined.add(method.getReturnType());
            }
        }
        for (Field field : JudoDefaultSpringConfiguration.class.getDeclaredFields()) {
            final Autowired autowired = field.getAnnotation(Autowired.class);
            final Class<?> type = field.getType();
            if (autowired == null || !autowired.required() || models.contains(type)
                    || defined.stream().anyMatch(type::isAssignableFrom)) {
                continue;
            }
            registerStub(context, type);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void registerStub(final AnnotationConfigApplicationContext context, final Class<T> type) {
        context.registerBean(type, () -> Mockito.mock(type));
    }

    /** Everything but the {@code QueryFactory} bean (and the singletons registered directly) is created on demand. */
    private static BeanFactoryPostProcessor lazyExceptQueryFactory() {
        return beanFactory -> {
            for (String name : beanFactory.getBeanDefinitionNames()) {
                final BeanDefinition definition = beanFactory.getBeanDefinition(name);
                if (definition.getFactoryMethodName() != null && !"getQueryFactory".equals(definition.getFactoryMethodName())) {
                    definition.setLazyInit(true);
                }
            }
        };
    }

    private static AnnotationConfigApplicationContext context(final String mode, final boolean ownModelBeans) {
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        final Map<String, Object> properties = new HashMap<>();
        properties.put("judo.modelName", NorthwindModelSet.shared().name);
        if (mode != null) {
            properties.put("judo.queryModel.mode", mode);
        }
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", properties));
        context.registerBean(Dialect.class, HsqldbDialect::new);
        registerStubs(context);
        context.addBeanFactoryPostProcessor(lazyExceptQueryFactory());
        if (!ownModelBeans) {
            context.register(JudoModelLoaderConfiguration.class, JudoModelConfiguration.class);
        }
        context.register(JudoDefaultSpringConfiguration.class);
        return context;
    }

    /** What a context start did: the log of the decision, and either the started context or the failure. */
    private static final class Started implements AutoCloseable {
        AnnotationConfigApplicationContext context;
        RuntimeException failure;
        List<String> info;
        List<String> warnings;

        String failureText() {
            final StringBuilder text = new StringBuilder();
            for (Throwable t = failure; t != null; t = t.getCause()) {
                text.append(t.getMessage()).append('\n');
            }
            return text.toString();
        }

        @Override
        public void close() {
            if (context != null) {
                context.close();
            }
        }
    }

    private static Started start(final AnnotationConfigApplicationContext context) {
        final Started started = new Started();
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            try {
                context.refresh();
                started.context = context;
            } catch (RuntimeException e) {
                started.failure = e;
                context.close();
            }
            started.info = log.only(Level.INFO);
            started.warnings = log.warnings();
        }
        return started;
    }

    @Test
    void tp24RestoredFromTheClasspathWithASingleExpressionModelBean() throws Exception {
        putOnClasspath(State.PRESENT);

        try (Started started = start(context(null, false))) {
            assertThat(started.failureText(), started.failure == null, equalTo(true));
            final QueryFactory queryFactory = started.context.getBean(QueryFactory.class);
            assertThat(queryFactory.isRestored(), equalTo(true));
            assertThat("INFO line with the duration", started.info, hasItem(containsString("query model restored")));
            assertThat(started.warnings, empty());

            // exactly one ExpressionModel bean, the loader's -expression.model: the same instance as before the change
            assertThat(started.context.getBeansOfType(ExpressionModel.class).values(), hasSize(1));
            assertThat(started.context.getBean(ExpressionModel.class),
                    sameInstance(started.context.getBean(JudoModelLoader.class).getExpressionModel()));

            // the optional locator bean is there, once
            assertThat(started.context.getBeansOfType(PersistedQueryModel.class).values(), hasSize(1));
            assertThat(started.context.getBean(PersistedQueryModel.class),
                    sameInstance(started.context.getBean(JudoModelLoader.class).getPersistedQueryModel()));
        }
    }

    @Test
    void noPersistedQueryModelBeanWithoutTheArtifact() throws Exception {
        putOnClasspath(State.ABSENT);

        try (Started started = start(context(null, false))) {
            assertThat(started.failureText(), started.failure == null, equalTo(true));
            assertThat(started.context.getBeansOfType(PersistedQueryModel.class).values(), empty());
            assertThat(started.context.getBeanProvider(PersistedQueryModel.class).getIfAvailable(), nullValue());
            assertThat("absent: the build runs, silently", started.warnings, empty());
            assertThat(started.context.getBean(QueryFactory.class).isRestored(), equalTo(false));
            assertThat(started.context.getBeansOfType(ExpressionModel.class).values(), hasSize(1));
        }
    }

    @TestFactory
    List<DynamicTest> everyStateInEveryMode() {
        final List<DynamicTest> tests = new ArrayList<>();
        for (State state : State.values()) {
            for (String mode : new String[]{null, "auto", "load", "build"}) {
                tests.add(DynamicTest.dynamicTest(state + " / " + (mode == null ? "default" : mode), () -> {
                    putOnClasspath(state);
                    try (Started started = start(context(mode, false))) {
                        verify(started, state, mode == null ? "auto" : mode);
                    } finally {
                        removeModelsFromClasspath();
                    }
                }));
            }
        }
        return tests;
    }

    private static void verify(final Started started, final State state, final String mode) {
        if ("build".equals(mode)) {
            assertThat(started.failureText(), started.failure == null, equalTo(true));
            // the artifact is never opened, so even a corrupt one is silent
            assertThat(started.context.getBean(QueryFactory.class).isRestored(), equalTo(false));
            assertThat(started.warnings, empty());
            assertThat(started.info, not(hasItem(containsString("restored"))));
            return;
        }
        if (state == State.PRESENT) {
            assertThat(started.failureText(), started.failure == null, equalTo(true));
            assertThat(started.context.getBean(QueryFactory.class).isRestored(), equalTo(true));
            assertThat(started.info, hasItem(containsString("query model restored")));
            assertThat(started.warnings, empty());
            return;
        }
        if ("load".equals(mode)) {
            assertThat("strict load: startup fails", started.failure != null, equalTo(true));
            assertThat(started.failureText(), containsString(state == State.ABSENT ? "missing"
                    : state == State.STALE ? "expressionDigest mismatch" : "Query model mode is load"));
            return;
        }
        assertThat(started.failureText(), started.failure == null, equalTo(true));
        assertThat(started.context.getBean(QueryFactory.class).isRestored(), equalTo(false));
        if (state == State.ABSENT) {
            assertThat(started.warnings, empty());
        } else {
            assertThat(started.warnings, hasSize(1));
            assertThat(started.warnings.get(0), containsString(state == State.STALE ? "expressionDigest mismatch" : "Persisted query model"));
        }
    }

    @Test
    void applicationWithItsOwnModelBeansBuilds() throws Exception {
        // the application loads (or generates) its models itself and defines the beans: no JudoModelLoader, no locator
        final JudoModelLoader models = ModelSources.load(ModelSources.Source.DIRECTORY, State.PRESENT);
        try {
            final AnnotationConfigApplicationContext context = context(null, true);
            context.registerBean(AsmModel.class, models::getAsmModel);
            context.registerBean(MeasureModel.class, models::getMeasureModel);
            context.registerBean(ExpressionModel.class, models::getExpressionModel);
            context.registerBean(RdbmsModel.class, models::getRdbmsModel);
            context.registerBean(Asm2RdbmsTransformationTrace.class, models::getAsm2rdbms);

            try (Started started = start(context)) {
                assertThat(started.failureText(), started.failure == null, equalTo(true));
                assertThat("no PersistedQueryModel bean", started.context.getBeansOfType(PersistedQueryModel.class).values(), empty());
                assertThat(started.context.getBean(QueryFactory.class).isRestored(), equalTo(false));
                assertThat(started.warnings, empty());
                assertThat(started.context.getBeansOfType(ExpressionModel.class).values(), hasSize(1));
            }
        } finally {
            ModelSources.cleanUp();
        }
    }
}
