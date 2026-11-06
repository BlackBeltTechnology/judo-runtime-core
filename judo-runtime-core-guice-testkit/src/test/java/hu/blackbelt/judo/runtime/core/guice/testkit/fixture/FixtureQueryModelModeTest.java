package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.QueryModelLoadOrBuild;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.sql.DataSource;

import static hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture.DIALECT_HSQLDB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Task 1.7: {@link JudoRuntimeFixture} obtains its {@code QueryFactory} through {@link QueryModelLoadOrBuild}, with a
 * configurable query model mode: {@code build} builds from JQL and never opens the artifact, {@code load} restores the
 * generated artifact or fails, {@code auto} (default) restores when it can.
 */
class FixtureQueryModelModeTest {

    private File directory;
    private DataSource dataSource;
    private final AtomicInteger openings = new AtomicInteger();
    private JudoRuntimeFixture fixture;

    @BeforeEach
    void setUp() throws Exception {
        directory = Files.createTempDirectory("judo-testkit-query-model").toFile();
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:querymodelmode" + System.nanoTime() + ";shutdown=true");
        config.setUsername("SA");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fixture != null) {
            fixture.tearDown();
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
        try (Stream<java.nio.file.Path> paths = Files.walk(directory.toPath())) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private JudoModelLoader models(final boolean withQueryModel, final boolean corruptQueryModel) throws Exception {
        final NorthwindModelSet set = NorthwindModelSet.shared();
        set.writeDirectory(directory, withQueryModel);
        if (corruptQueryModel) {
            final File query = new File(directory, set.queryModelFileName);
            Files.write(query.toPath(), Arrays.copyOf(set.queryModel, set.queryModel.length / 2));
        }
        final JudoModelLoader loaded = JudoModelLoader.loadFromDirectory(set.name, directory, new HsqldbDialect(), false);
        if (loaded.getPersistedQueryModel() == null) {
            return loaded;
        }
        // the same models, with a locator that counts how often the artifact is opened
        final PersistedQueryModel locator = loaded.getPersistedQueryModel();
        return JudoModelLoader.builder()
                .asmModel(loaded.getAsmModel())
                .rdbmsModel(loaded.getRdbmsModel())
                .measureModel(loaded.getMeasureModel())
                .expressionModel(loaded.getExpressionModel())
                .liquibaseModel(loaded.getLiquibaseModel())
                .asm2rdbms(loaded.getAsm2rdbms())
                .asmDigest(loaded.getAsmDigest())
                .measureDigest(loaded.getMeasureDigest())
                .expressionDigest(loaded.getExpressionDigest())
                .persistedQueryModel(new PersistedQueryModel(locator.getName(), () -> {
                    openings.incrementAndGet();
                    return locator.openStream();
                }))
                .build();
    }

    @Test
    void buildModeBuildsFromJqlAndDoesNotOpenTheArtifact() throws Exception {
        // a corrupt artifact: it would not restore, and opening it is counted
        final JudoModelLoader loader = models(true, true);
        fixture = new JudoRuntimeFixture();
        fixture.queryModelMode = "build";

        fixture.prepareWithModel(loader, dataSource, DIALECT_HSQLDB);

        assertThat(fixture.queryFactory().isRestored(), equalTo(false));
        assertThat("built from the JQL of the model", fixture.queryFactory().getEntityTypeExpressionsMap().isEmpty(), equalTo(false));
        assertThat("the artifact is not opened", openings.get(), equalTo(0));
    }

    @Test
    void strictLoadModeRestoresTheGeneratedArtifact() throws Exception {
        final JudoModelLoader loader = models(true, false);
        fixture = new JudoRuntimeFixture();
        fixture.queryModelMode = "load";

        fixture.prepareWithModel(loader, dataSource, DIALECT_HSQLDB);

        assertThat(fixture.queryFactory().isRestored(), equalTo(true));
        assertThat("the artifact is opened once, by the decision", openings.get(), equalTo(1));
    }

    @Test
    void strictLoadModeWithoutTheArtifactFails() throws Exception {
        final JudoModelLoader loader = models(false, false);
        fixture = new JudoRuntimeFixture();
        fixture.queryModelMode = "load";

        final IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> fixture.prepareWithModel(loader, dataSource, DIALECT_HSQLDB));

        assertThat(failure.getMessage(), containsString("missing"));
        assertThat("no artifact, nothing to open", openings.get(), equalTo(0));
    }

    @Test
    void defaultModeRestoresWhenTheArtifactIsUsable() throws Exception {
        final JudoModelLoader loader = models(true, false);
        fixture = new JudoRuntimeFixture();

        fixture.prepareWithModel(loader, dataSource, DIALECT_HSQLDB);

        assertThat(fixture.queryModelMode, equalTo(System.getProperty(JudoRuntimeFixture.QUERY_MODEL_MODE_PROPERTY, "auto")));
        assertThat(fixture.queryFactory().isRestored(), equalTo(true));
        assertThat(openings.get(), equalTo(1));
    }
}
