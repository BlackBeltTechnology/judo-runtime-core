package hu.blackbelt.judo.runtime.core.guice;

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

import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.runtime.core.guice.ModelSources.Source;
import hu.blackbelt.judo.runtime.core.guice.ModelSources.State;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.query.QueryModelLoadOrBuild;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import ch.qos.logback.classic.Level;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tasks 1.5 and 2.5 (TP-25): {@link JudoDefaultModule} wiring of the query model: no second {@code ExpressionModel}, no
 * query-side binding, the {@code queryModelMode} option, the supplied {@link QueryFactory} override.
 */
class JudoDefaultModuleQueryModelTest {

    @AfterAll
    static void cleanUp() {
        ModelSources.cleanUp();
    }

    /** Only the singletons that are eager in production are created at injector creation; the rest on first request. */
    private static Injector injector(final JudoDefaultModule module) {
        try {
            return Guice.createInjector(Stage.DEVELOPMENT, module, new TestBindings());
        } catch (com.google.inject.CreationException e) {
            // Guice formats its own message with a bytecode reader that cannot read this JDK's class files: use the raw messages
            final StringBuilder text = new StringBuilder("The injector cannot be created:");
            e.getErrorMessages().forEach(m -> text.append("\n  ").append(m.getMessage()));
            throw new IllegalStateException(text.toString());
        }
    }

    @Test
    void tp25NoSecondExpressionModel() throws Exception {
        final JudoModelLoader loader = ModelSources.load(Source.DIRECTORY, State.PRESENT);
        assertThat("the artifacts are present", loader.getPersistedQueryModel() != null, equalTo(true));

        final Injector injector = injector(JudoDefaultModule.builder().judoModelLoader(loader).build());

        assertThat(injector.getInstance(ExpressionModel.class), sameInstance(loader.getExpressionModel()));
        final List<Binding<ExpressionModel>> bindings = injector.findBindingsByType(TypeLiteral.get(ExpressionModel.class));
        assertThat("exactly one ExpressionModel binding", bindings, hasSize(1));
        assertThat("the query-side model is not bound", injector.getExistingBinding(
                Key.get(hu.blackbelt.judo.meta.query.runtime.QueryModel.class)), nullValue());
        assertThat("the locator is not bound either: only the loader carries it", injector.getExistingBinding(
                Key.get(PersistedQueryModel.class)), nullValue());
    }

    @Test
    void defaultModeIsAuto() {
        assertThat(JudoDefaultModuleConfiguration.DEFAULT.getQueryModelMode(), equalTo("auto"));
        assertThat(JudoDefaultModule.builder().judoModelLoader(null).build().getConfiguration().getQueryModelMode(), equalTo("auto"));
    }

    @Test
    void builderCopiesTheMode() {
        final JudoDefaultModule module = JudoDefaultModule.builder().queryModelMode("load").build();

        assertThat(module.getConfiguration().getQueryModelMode(), equalTo("load"));
    }

    @Test
    void configurationTakesPrecedenceOverTheBuilderMode() {
        final JudoDefaultModule module = JudoDefaultModule.builder()
                .configuration(JudoDefaultModuleConfiguration.builder().queryModelMode("build").build())
                .queryModelMode("load")
                .build();

        assertThat(module.getConfiguration().getQueryModelMode(), equalTo("build"));
    }

    @Test
    void configuredModeReachesTheDecision() throws Exception {
        final JudoModelLoader loader = ModelSources.load(Source.DIRECTORY, State.ABSENT);

        // load without the artifact: the decision fails, which only happens when the mode went through the module;
        // it fails at injector creation, because the QueryFactory is created eagerly (the eager Dispatcher needs it)
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> injector(JudoDefaultModule.builder().judoModelLoader(loader).queryModelMode("load").build()));

        assertThat(failure.getMessage(), containsString("Query model mode is load"));
        assertThat(failure.getMessage(), containsString("missing"));
    }

    @Test
    void queryFactoryIsRestoredEagerlyAtInjectorCreation() throws Exception {
        final JudoModelLoader loader = ModelSources.load(Source.DIRECTORY, State.PRESENT);

        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            final Injector injector = injector(JudoDefaultModule.builder().judoModelLoader(loader).build());

            assertThat("restored while the injector was created, before any getInstance", log.only(Level.INFO),
                    hasItem(containsString("query model restored")));
            assertThat(injector.getInstance(QueryFactory.class).isRestored(), equalTo(true));
        }
    }

    @Test
    void suppliedQueryFactoryBypassesTheDecision() throws Exception {
        final JudoModelLoader loader = ModelSources.load(Source.DIRECTORY, State.PRESENT);
        final QueryFactory supplied = injector(JudoDefaultModule.builder().judoModelLoader(loader).build()).getInstance(QueryFactory.class);

        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            // strict load would restore again and log: it is never asked
            final Injector injector = injector(JudoDefaultModule.builder().judoModelLoader(loader).queryFactory(supplied).queryModelMode("load").build());

            assertThat(injector.getInstance(QueryFactory.class), sameInstance(supplied));
            assertThat("the decision was not called", log.atLeast(Level.INFO), empty());
        }
    }
}
