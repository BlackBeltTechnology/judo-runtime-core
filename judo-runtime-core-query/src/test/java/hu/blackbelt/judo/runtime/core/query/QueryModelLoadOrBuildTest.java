package hu.blackbelt.judo.runtime.core.query;

/*-
 * #%L
 * JUDO Runtime Core :: Query
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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The decision table of {@link QueryModelLoadOrBuild} (design D5): every row in every mode, over the Northwind models.
 *
 * <p>The tests of the individual rows (TP-29..TP-35, TP-38..TP-43) pin the log text, the matrix
 * ({@link #everyRowInEveryMode()}) pins the outcome of every row x mode.</p>
 */
public class QueryModelLoadOrBuildTest {

    private static final String GENERATOR_RESOURCE_IDENTITY = QueryModelPersistence.generatorIdentity();

    private final SavedQueryModel saved = SavedQueryModel.shared();

    /**
     * Counts the builds of the decision, and answers the shared built factory (the restored one must not be it).
     */
    private final AtomicInteger builds = new AtomicInteger();

    private QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder decision(final PersistedQueryModel locator, final QueryModelLoadOrBuild.Mode mode) {
        return saved.decision(locator).mode(mode).buildQueryFactory(() -> {
            builds.incrementAndGet();
            return saved.built;
        });
    }

    private static List<String> warnings(final LogCapture log) {
        return log.warnings();
    }

    // ------------------------------------------------------------------------------------------------------------
    // The restore row
    // ------------------------------------------------------------------------------------------------------------

    /**
     * Valid artifact in mode auto: restored, INFO line with the duration, no build, no WARN, nothing unresolved.
     */
    @Test
    void validArtifactIsRestored() {
        final AtomicInteger opened = new AtomicInteger();
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            final QueryFactory factory = decision(saved.locator(saved.artifact, opened), QueryModelLoadOrBuild.Mode.AUTO).get();

            assertThat(factory.isRestored(), equalTo(true));
            assertThat(builds.get(), equalTo(0));
            assertThat(opened.get(), equalTo(1));
            assertThat(warnings(log), empty());
            assertThat(log.only(Level.INFO), hasItem(matchesPattern(".*query model restored in \\d+ ms.*")));
            // serves queries, equal to the built ones
            final EClass mapped = saved.fixture.asmUtils.all(EClass.class).filter(saved.fixture.asmUtils::isMappedTransferObjectType)
                    .filter(c -> saved.built.getQuery(c).isPresent()).findFirst().orElseThrow();
            assertThat(factory.getQuery(mapped).isPresent(), equalTo(true));
        }
    }

    /**
     * The default mode is auto, and the mode is parsed case-insensitively; an unknown value is refused.
     */
    @Test
    void modeIsParsedAndDefaultsToAuto() {
        assertThat(QueryModelLoadOrBuild.Mode.parse(null), equalTo(QueryModelLoadOrBuild.Mode.AUTO));
        assertThat(QueryModelLoadOrBuild.Mode.parse(" "), equalTo(QueryModelLoadOrBuild.Mode.AUTO));
        assertThat(QueryModelLoadOrBuild.Mode.parse("Load"), equalTo(QueryModelLoadOrBuild.Mode.LOAD));
        assertThat(QueryModelLoadOrBuild.Mode.parse("BUILD"), equalTo(QueryModelLoadOrBuild.Mode.BUILD));
        assertThrows(IllegalArgumentException.class, () -> QueryModelLoadOrBuild.Mode.parse("sometimes"));

        final AtomicInteger opened = new AtomicInteger();
        final QueryFactory factory = saved.decision(saved.locator(saved.artifact, opened))
                .buildQueryFactory(() -> saved.built).get(); // no mode set: auto
        assertThat(factory.isRestored(), equalTo(true));
        final QueryFactory fromText = saved.decision(saved.locator(saved.artifact, opened)).mode("LOAD")
                .buildQueryFactory(() -> saved.built).get();
        assertThat(fromText.isRestored(), equalTo(true));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Fallback rows (TP-29 .. TP-43), mode auto
    // ------------------------------------------------------------------------------------------------------------

    /**
     * TP-29: no query file: the build runs, no WARN, and the factory serves queries (the container's real build).
     */
    @Test
    void artifactAbsentBuilds() {
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            final QueryFactory factory = saved.decision(null).mode(QueryModelLoadOrBuild.Mode.AUTO)
                    .buildQueryFactory(() -> {
                        builds.incrementAndGet();
                        return QueryFactory.builder()
                                .asmResourceSet(saved.fixture.asmModel.getResourceSet())
                                .measureResourceSet(saved.fixture.measureModel.getResourceSet())
                                .expressionResourceSet(saved.fixture.expressionModel.getResourceSet())
                                .build();
                    }).get();

            assertThat(builds.get(), equalTo(1));
            assertThat(factory.isRestored(), equalTo(false));
            assertThat(warnings(log), empty());
            final EClass mapped = saved.fixture.asmUtils.all(EClass.class).filter(saved.fixture.asmUtils::isMappedTransferObjectType)
                    .filter(c -> saved.built.getQuery(c).isPresent()).findFirst().orElseThrow();
            assertThat(factory.getQuery(mapped).isPresent(), equalTo(true));
        }
    }

    /**
     * TP-30: the ASM file differs from the one the artifact was built from (one JQL text changed).
     */
    @Test
    void asmDigestMismatchBuilds() {
        final String changed = QueryModelPersistence.sha256Hex(append(saved.asmBytes, " "));
        assertWarnedAndBuilt(saved.decision(saved.locator(saved.artifact, new AtomicInteger())).asmDigest(changed), "asmDigest mismatch");
    }

    /**
     * TP-31: one byte appended to the measure file.
     */
    @Test
    void measureDigestMismatchBuilds() {
        final String changed = QueryModelPersistence.sha256Hex(append(saved.measureBytes, "\n"));
        assertWarnedAndBuilt(saved.decision(saved.locator(saved.artifact, new AtomicInteger())).measureDigest(changed), "measureDigest mismatch");
    }

    /**
     * TP-32: the expression file is the one of another run of the same model.
     */
    @Test
    void expressionDigestMismatchBuilds() {
        final SavedQueryModel otherRun = new SavedQueryModel(NorthwindFixture.create());
        assertThat(otherRun.expressionDigest, not(equalTo(saved.expressionDigest)));
        assertWarnedAndBuilt(saved.decision(saved.locator(saved.artifact, new AtomicInteger())).expressionDigest(otherRun.expressionDigest),
                "expressionDigest mismatch");
    }

    /**
     * TP-33: the artifact was generated by another build of judo-tatami-asm2query; the WARN names both identities.
     */
    @Test
    void generatorMismatchBuilds() {
        final String foreign = "judo-tatami-asm2query:1.1.6-SNAPSHOT@20261101_000000";
        assertThat(GENERATOR_RESOURCE_IDENTITY, not(equalTo(foreign)));
        final String warning = assertWarnedAndBuilt(
                saved.decision(saved.locator(saved.withInfoAttribute("generator", foreign), new AtomicInteger())), "generator mismatch");
        assertThat(warning, containsString(foreign));
        assertThat(warning, containsString(GENERATOR_RESOURCE_IDENTITY));
    }

    /**
     * TP-34 (a): the model name of the artifact differs from the ASM model's.
     */
    @Test
    void modelNameMismatchBuilds() {
        final String warning = assertWarnedAndBuilt(
                saved.decision(saved.locator(saved.withInfoAttribute("modelName", "OtherModel"), new AtomicInteger())), "modelName mismatch");
        assertThat(warning, containsString("OtherModel"));
        assertThat(warning, containsString(saved.name));
    }

    /**
     * TP-34 (b): the format version of the artifact is the supported one plus one.
     */
    @Test
    void formatVersionMismatchBuilds() {
        final String warning = assertWarnedAndBuilt(
                saved.decision(saved.locator(saved.withInfoAttribute("formatVersion", "2"), new AtomicInteger())), "formatVersion mismatch");
        assertThat(warning, containsString("artifact 2"));
        assertThat(warning, containsString("expected 1"));
    }

    /**
     * TP-35: the artifact is cut at 50 %; the WARN has the parse cause, the boot goes on with the build.
     */
    @Test
    void corruptXmiBuilds() {
        final byte[] truncated = Arrays.copyOf(saved.artifact, saved.artifact.length / 2);
        final String warning = assertWarnedAndBuilt(saved.decision(saved.locator(truncated, new AtomicInteger())), "restoring the saved query model failed");
        assertThat(warning, containsString("XML document structures must start and end within the same entity"));
    }

    /**
     * TP-38: the runtime ASM lacks one mapped entity the artifact points to; the WARN has the proxy count (N >= 1).
     */
    @Test
    void unresolvedProxiesBuild() {
        final SavedQueryModel own = new SavedQueryModel(NorthwindFixture.create());
        final EClass entity = own.fixture.asmUtils.all(EClass.class).filter(own.fixture.asmUtils::isMappedTransferObjectType)
                .map(c -> own.built.getQuery(c).map(select -> select.getFrom()).orElse(null))
                .filter(java.util.Objects::nonNull).findFirst().orElseThrow();
        final String entityName = AsmUtils.getClassifierFQName(entity);
        entity.getEPackage().getEClassifiers().remove(entity);

        final AtomicInteger ownBuilds = new AtomicInteger();
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            // the digests are the ones of the artifact: nothing but the missing entity differs
            own.decision(own.locator(own.artifact, new AtomicInteger())).mode(QueryModelLoadOrBuild.Mode.AUTO)
                    .buildQueryFactory(() -> {
                        ownBuilds.incrementAndGet();
                        return own.built;
                    }).get();

            assertThat(ownBuilds.get(), equalTo(1));
            assertThat(log.warnings(), hasSize(1));
            final java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("unresolved proxies: (\\d+)").matcher(log.warnings().get(0));
            assertThat(entityName + " removed: " + log.warnings(), matcher.find(), equalTo(true));
            assertThat(Integer.parseInt(matcher.group(1)), greaterThan(0));
        }
    }

    /**
     * TP-39: the stream fails with an I/O error after 1 KB; the WARN has the cause, the boot goes on.
     */
    @Test
    void ioErrorMidStreamBuilds() {
        final PersistedQueryModel.StreamSupplier failing = () -> new InputStream() {
            private final InputStream delegate = new ByteArrayInputStream(saved.artifact);
            private int count;

            @Override
            public int read() throws IOException {
                if (count++ >= 1024) {
                    throw new IOException("disk gone");
                }
                return delegate.read();
            }
        };
        final String warning = assertWarnedAndBuilt(saved.decision(saved.locator(failing, new AtomicInteger())), "restoring the saved query model failed");
        assertThat(warning, containsString("disk gone"));
    }

    /**
     * TP-40: an {@link Error} (a class that cannot be loaded) while restoring is a fallback too, not a crash.
     */
    @Test
    void errorDuringRestoreBuilds() {
        final PersistedQueryModel.StreamSupplier failing = () -> new InputStream() {
            @Override
            public int read() {
                throw new NoClassDefFoundError("injected/Missing");
            }
        };
        final String warning = assertWarnedAndBuilt(saved.decision(saved.locator(failing, new AtomicInteger())), "restoring the saved query model failed");
        assertThat(warning, containsString("NoClassDefFoundError"));
    }

    /**
     * TP-41: custom joins are configured: INFO (not WARN), the artifact is not even opened, the build runs.
     */
    @Test
    void customJoinsBuild() {
        final AtomicInteger opened = new AtomicInteger();
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            decision(saved.locator(saved.artifact, opened), QueryModelLoadOrBuild.Mode.AUTO).customJoinDefinitions(oneCustomJoin()).get();

            assertThat(builds.get(), equalTo(1));
            assertThat(opened.get(), equalTo(0));
            assertThat(warnings(log), empty());
            assertThat(log.only(Level.INFO), hasItem(containsString("saved query model not used with custom joins")));
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Mode build (TP-42) and strict load (TP-43)
    // ------------------------------------------------------------------------------------------------------------

    /**
     * TP-42: mode build never opens the artifact, whatever it holds.
     */
    @Test
    void buildModeNeverOpensTheArtifact() {
        final AtomicInteger opened = new AtomicInteger();
        final PersistedQueryModel corrupt = saved.locator("not xml at all".getBytes(java.nio.charset.StandardCharsets.UTF_8), opened);
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            decision(corrupt, QueryModelLoadOrBuild.Mode.BUILD).get();

            assertThat(builds.get(), equalTo(1));
            assertThat(opened.get(), equalTo(0));
            assertThat(warnings(log), empty());
        }
    }

    /**
     * TP-43 (a): strict load with a generator mismatch fails, naming both generator identities.
     */
    @Test
    void strictLoadFailsOnGeneratorMismatch() {
        final String foreign = "judo-tatami-asm2query:1.1.6-SNAPSHOT@20261101_000000";
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> decision(
                saved.locator(saved.withInfoAttribute("generator", foreign), new AtomicInteger()), QueryModelLoadOrBuild.Mode.LOAD).get());
        assertThat(e.getMessage(), containsString(foreign));
        assertThat(e.getMessage(), containsString(GENERATOR_RESOURCE_IDENTITY));
        assertThat(builds.get(), equalTo(0));
    }

    /**
     * TP-43 (b): strict load without a query file fails, saying it is missing.
     */
    @Test
    void strictLoadFailsWithoutTheArtifact() {
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> decision(null, QueryModelLoadOrBuild.Mode.LOAD).get());
        assertThat(e.getMessage(), containsString("missing"));
        assertThat(builds.get(), equalTo(0));
    }

    /**
     * TP-43 (c): strict load with custom joins fails, saying the saved query model cannot be used with them.
     */
    @Test
    void strictLoadFailsWithCustomJoins() {
        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> decision(
                saved.locator(saved.artifact, new AtomicInteger()), QueryModelLoadOrBuild.Mode.LOAD).customJoinDefinitions(oneCustomJoin()).get());
        assertThat(e.getMessage(), containsString("cannot be used with custom joins"));
        assertThat(builds.get(), equalTo(0));
    }

    // ------------------------------------------------------------------------------------------------------------
    // Every row x every mode
    // ------------------------------------------------------------------------------------------------------------

    /**
     * One row of the table: the inputs of the decision, and what mode auto and mode load answer.
     */
    private static final class Row {
        final String name;
        final java.util.function.Function<SavedQueryModel, QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder> inputs;
        /**
         * Text of the WARN or INFO line, {@code null} for no line.
         */
        final String autoLine;
        final Level autoLevel;
        /**
         * Text in the error of mode load, {@code null} when mode load restores.
         */
        final String loadError;

        Row(final String name, final java.util.function.Function<SavedQueryModel, QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder> inputs,
            final String autoLine, final Level autoLevel, final String loadError) {
            this.name = name;
            this.inputs = inputs;
            this.autoLine = autoLine;
            this.autoLevel = autoLevel;
            this.loadError = loadError;
        }
    }

    private List<Row> rows() {
        final String foreign = "judo-tatami-asm2query:1.1.6-SNAPSHOT@20261101_000000";
        final List<Row> rows = new ArrayList<>();
        rows.add(new Row("valid", s -> s.decision(s.locator(s.artifact, new AtomicInteger())),
                "query model restored", Level.INFO, null));
        rows.add(new Row("custom joins", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).customJoinDefinitions(oneCustomJoin()),
                "saved query model not used with custom joins", Level.INFO, "cannot be used with custom joins"));
        rows.add(new Row("no locator", s -> s.decision(null), "missing", Level.INFO, "missing"));
        rows.add(new Row("no expression model", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).expressionModel(null),
                "expression model", Level.INFO, "expression model"));
        rows.add(new Row("no ASM digest", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).asmDigest(null),
                "digest of the ASM model file is not available", Level.INFO, "digest of the ASM model file is not available"));
        rows.add(new Row("failure", s -> s.decision(s.locator(Arrays.copyOf(s.artifact, s.artifact.length / 2), new AtomicInteger())),
                "restoring the saved query model failed", Level.WARN, "restoring the saved query model failed"));
        rows.add(new Row("error", s -> s.decision(s.locator(() -> {
            throw new NoClassDefFoundError("injected/Missing");
        }, new AtomicInteger())), "NoClassDefFoundError", Level.WARN, "NoClassDefFoundError"));
        rows.add(new Row("null stream", s -> s.decision(s.locator(() -> null, new AtomicInteger())), "missing", Level.INFO, "missing"));
        rows.add(new Row("generator", s -> s.decision(s.locator(s.withInfoAttribute("generator", foreign), new AtomicInteger())),
                "generator mismatch", Level.WARN, foreign));
        rows.add(new Row("modelName", s -> s.decision(s.locator(s.withInfoAttribute("modelName", "OtherModel"), new AtomicInteger())),
                "modelName mismatch", Level.WARN, "OtherModel"));
        rows.add(new Row("formatVersion", s -> s.decision(s.locator(s.withInfoAttribute("formatVersion", "2"), new AtomicInteger())),
                "formatVersion mismatch", Level.WARN, "formatVersion mismatch"));
        rows.add(new Row("asmDigest", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).asmDigest(QueryModelPersistence.sha256Hex(append(s.asmBytes, " "))),
                "asmDigest mismatch", Level.WARN, "asmDigest mismatch"));
        rows.add(new Row("measureDigest", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).measureDigest(QueryModelPersistence.sha256Hex(append(s.measureBytes, "\n"))),
                "measureDigest mismatch", Level.WARN, "measureDigest mismatch"));
        rows.add(new Row("no measure digest", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).measureModel(null).measureDigest(null),
                "measureDigest mismatch", Level.WARN, "measureDigest mismatch")); // the artifact was stamped with a measure model
        rows.add(new Row("expressionDigest", s -> s.decision(s.locator(s.artifact, new AtomicInteger())).expressionDigest(QueryModelPersistence.sha256Hex(append(s.expressionBytes, " "))),
                "expressionDigest mismatch", Level.WARN, "expressionDigest mismatch"));
        return rows;
    }

    /**
     * Every row of the table in the three modes: build mode builds and never opens the artifact; auto restores or builds with the
     * expected line; load restores or fails with the reason.
     */
    @TestFactory
    List<DynamicTest> everyRowInEveryMode() {
        final List<DynamicTest> tests = new ArrayList<>();
        for (Row row : rows()) {
            for (QueryModelLoadOrBuild.Mode mode : QueryModelLoadOrBuild.Mode.values()) {
                tests.add(DynamicTest.dynamicTest(row.name + " / " + mode, () -> runRow(row, mode)));
            }
        }
        return tests;
    }

    private void runRow(final Row row, final QueryModelLoadOrBuild.Mode mode) {
        final AtomicInteger rowBuilds = new AtomicInteger();
        final QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder inputs = row.inputs.apply(saved).mode(mode)
                .buildQueryFactory(() -> {
                    rowBuilds.incrementAndGet();
                    return saved.built;
                });

        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            if (mode == QueryModelLoadOrBuild.Mode.BUILD) {
                assertThat(inputs.get(), equalTo(saved.built));
                assertThat(rowBuilds.get(), equalTo(1));
                assertThat(log.atLeast(Level.INFO), empty());
            } else if (mode == QueryModelLoadOrBuild.Mode.AUTO) {
                final QueryFactory factory = inputs.get();
                final List<String> lines = log.atLeast(Level.INFO);
                assertThat(row.name, lines, hasSize(1));
                assertThat(row.name, lines.get(0), containsString(row.autoLine));
                assertThat(row.name, log.only(row.autoLevel), hasSize(1));
                if (row.loadError == null) {
                    assertThat(row.name, factory.isRestored(), equalTo(true));
                    assertThat(row.name, rowBuilds.get(), equalTo(0));
                } else {
                    assertThat(row.name, factory, equalTo(saved.built));
                    assertThat(row.name, rowBuilds.get(), equalTo(1));
                }
            } else if (row.loadError == null) {
                assertThat(row.name, inputs.get().isRestored(), equalTo(true));
                assertThat(row.name, rowBuilds.get(), equalTo(0));
            } else {
                final IllegalStateException e = assertThrows(IllegalStateException.class, inputs::get, row.name);
                assertThat(row.name, e.getMessage(), containsString(row.loadError));
                assertThat(row.name, rowBuilds.get(), equalTo(0));
                assertThat(row.name, log.warnings(), empty());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------

    /**
     * Runs mode auto over the inputs, expects exactly one WARN containing the text and one build.
     *
     * @return the WARN text
     */
    private String assertWarnedAndBuilt(final QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder inputs, final String text) {
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class)) {
            final QueryFactory factory = inputs.mode(QueryModelLoadOrBuild.Mode.AUTO)
                    .buildQueryFactory(() -> {
                        builds.incrementAndGet();
                        return saved.built;
                    }).get();

            assertThat(factory, equalTo(saved.built));
            assertThat(builds.get(), equalTo(1));
            assertThat(log.warnings(), hasSize(1));
            assertThat(log.warnings().get(0), containsString(text));
            assertThat(log.warnings().get(0), notNullValue());
            return log.warnings().get(0);
        }
    }

    private Map<EReference, CustomJoinDefinition> oneCustomJoin() {
        final Map<EReference, CustomJoinDefinition> joins = new HashMap<>();
        joins.put(saved.fixture.asmUtils.all(EReference.class).findFirst().orElseThrow(),
                CustomJoinDefinition.builder().navigationSql("select 1").build());
        return joins;
    }

    private static byte[] append(final byte[] bytes, final String text) {
        final byte[] extra = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final byte[] result = Arrays.copyOf(bytes, bytes.length + extra.length);
        System.arraycopy(extra, 0, result, bytes.length, extra.length);
        return result;
    }
}
