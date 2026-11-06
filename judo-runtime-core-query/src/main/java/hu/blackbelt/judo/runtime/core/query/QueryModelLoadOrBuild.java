package hu.blackbelt.judo.runtime.core.query;

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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EReference;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The one place that decides between restoring a persisted query model and building it.
 *
 * <p>Every runtime (Guice, Spring, OSGi, test fixtures) calls it. The decision (design D5):</p>
 * <pre>
 * mode = build                                         -&gt; build (the locator is never opened)
 * custom joins non-empty                               -&gt; build (INFO)                 | load: fail
 * no locator / artifact or input digest missing        -&gt; build                        | load: fail
 * any Exception or Error while reading, parsing,       -&gt; build (WARN, cause)          | load: fail
 *   resolving, validating or restoring
 * unresolved proxies &gt; 0                               -&gt; build (WARN, count)          | load: fail
 * formatVersion / generator / modelName mismatch       -&gt; build (WARN, both values)    | load: fail
 * asmDigest / measureDigest / expressionDigest         -&gt; build (WARN, which file)     | load: fail
 *   mismatch
 * otherwise                                            -&gt; restore (INFO with duration)
 * </pre>
 *
 * <p>{@code build} is the container's own build, passed as {@code buildQueryFactory} of the builder; it is the only code that
 * constructs a query model from JQL. Usage:</p>
 * <pre>{@code
 * QueryFactory queryFactory = QueryModelLoadOrBuild.queryModelLoadOrBuild()
 *         .asmModel(asmModel).asmDigest(asmDigest)
 *         .measureModel(measureModel).measureDigest(measureDigest)      // null when there is no measure model
 *         .expressionModel(expressionModel).expressionDigest(expressionDigest)
 *         .persistedQueryModel(locator)                                  // null when there is no query file
 *         .customJoinDefinitions(customJoinDefinitions)
 *         .mode("auto")
 *         .buildQueryFactory(() -> new QueryFactory(...))
 *         .get();
 * }</pre>
 *
 * <p>The digests are the SHA-256 hex of the complete bytes of each model file as loaded (see
 * {@code QueryModelPersistence.DigestInputStream}); the restore additionally compares the generator identity of
 * {@code judo-tatami-asm2query}, the model name and the format version stored in the artifact.</p>
 */
@Slf4j
public final class QueryModelLoadOrBuild implements Supplier<QueryFactory> {

    /**
     * Mode of the decision.
     */
    public enum Mode {
        /**
         * Restore a valid persisted query model, otherwise build (default).
         */
        AUTO,
        /**
         * Restore the persisted query model, fail when it cannot be used.
         */
        LOAD,
        /**
         * Always build; the persisted query model is never opened.
         */
        BUILD;

        /**
         * Parse a mode, case-insensitively.
         *
         * @param value {@code auto}, {@code load}, {@code build}; {@code null} or blank means {@code auto}
         * @return mode
         * @throws IllegalArgumentException for any other value
         */
        public static Mode parse(final String value) {
            if (value == null || value.trim().isEmpty()) {
                return AUTO;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported query model mode: '" + value + "', expected auto, load or build");
            }
        }
    }

    private final AsmModel asmModel;
    private final String asmDigest;
    private final MeasureModel measureModel;
    private final String measureDigest;
    private final ExpressionModel expressionModel;
    private final String expressionDigest;
    private final PersistedQueryModel persistedQueryModel;
    private final Map<EReference, CustomJoinDefinition> customJoinDefinitions;
    private final Mode mode;
    private final Supplier<QueryFactory> buildQueryFactory;

    /**
     * @param asmModel              live ASM model
     * @param asmDigest             SHA-256 hex of the complete ASM file as loaded ({@code null}: unknown, no restore)
     * @param measureModel          live measure model, {@code null} when there is none
     * @param measureDigest         SHA-256 hex of the measure file, {@code null} when there is no measure model
     * @param expressionModel       live expression model (the loaded {@code <name>-expression.model}); {@code null}: no restore
     * @param expressionDigest      SHA-256 hex of the complete expression file as loaded ({@code null}: unknown, no restore)
     * @param persistedQueryModel   locator of the query artifact, {@code null} when there is no query file; never opened before
     *                              the decision, and never in mode {@code build}
     * @param customJoinDefinitions custom joins of the runtime ({@code null} or empty: none)
     * @param mode                  mode, {@link Mode#AUTO} when {@code null}
     * @param buildQueryFactory     the container's existing build of the query factory
     */
    @Builder(builderMethodName = "queryModelLoadOrBuild")
    private QueryModelLoadOrBuild(@NonNull final AsmModel asmModel,
                                  final String asmDigest,
                                  final MeasureModel measureModel,
                                  final String measureDigest,
                                  final ExpressionModel expressionModel,
                                  final String expressionDigest,
                                  final PersistedQueryModel persistedQueryModel,
                                  final Map<EReference, CustomJoinDefinition> customJoinDefinitions,
                                  final Mode mode,
                                  @NonNull final Supplier<QueryFactory> buildQueryFactory) {
        this.asmModel = asmModel;
        this.asmDigest = asmDigest;
        this.measureModel = measureModel;
        this.measureDigest = measureDigest;
        this.expressionModel = expressionModel;
        this.expressionDigest = expressionDigest;
        this.persistedQueryModel = persistedQueryModel;
        this.customJoinDefinitions = customJoinDefinitions;
        this.mode = mode != null ? mode : Mode.AUTO;
        this.buildQueryFactory = buildQueryFactory;
    }

    /**
     * Builder additions: the mode from its configuration value.
     */
    public static class QueryModelLoadOrBuildBuilder {
        /**
         * @param mode {@code auto}, {@code load} or {@code build} (case-insensitive); {@code null} means {@code auto}
         * @return this builder
         * @throws IllegalArgumentException for an unsupported value
         */
        public QueryModelLoadOrBuildBuilder mode(final String mode) {
            this.mode = Mode.parse(mode);
            return this;
        }

        /**
         * @param mode mode
         * @return this builder
         */
        public QueryModelLoadOrBuildBuilder mode(final Mode mode) {
            this.mode = mode;
            return this;
        }

        /**
         * Shortcut of {@code build().get()}: take the decision and return the query factory.
         *
         * @return query factory
         * @see QueryModelLoadOrBuild#get()
         */
        public QueryFactory get() {
            return build().get();
        }
    }

    /**
     * Take the decision and return the query factory: restored, or built by the container's build.
     *
     * @return query factory
     * @throws IllegalStateException in mode {@code load} when the persisted query model cannot be used (the message names
     *                               the reason), and whatever the container's build throws
     */
    @Override
    public QueryFactory get() {
        if (mode == Mode.BUILD) {
            log.debug("Query model mode is build, the query model is built");
            return buildQueryFactory.get();
        }

        if (customJoinDefinitions != null && !customJoinDefinitions.isEmpty()) {
            if (mode == Mode.LOAD) {
                throw new IllegalStateException("The saved query model cannot be used with custom joins (query model mode load, "
                        + customJoinDefinitions.size() + " configured)");
            }
            log.info("saved query model not used with custom joins ({} configured), the query model is built", customJoinDefinitions.size());
            return buildQueryFactory.get();
        }

        if (persistedQueryModel == null) {
            return fallback("the saved query model is missing (no query model file)", null, false);
        }
        if (expressionModel == null) {
            return fallback("the saved query model is missing its expression model (no expression model loaded)", null, false);
        }
        if (asmDigest == null || expressionDigest == null) {
            return fallback("the digest of the " + (asmDigest == null ? "ASM" : "expression") + " model file is not available", null, false);
        }

        final long start = System.nanoTime();
        final Attempt attempt;
        try {
            attempt = restore();
        } catch (Exception | Error e) {
            return fallback("restoring the saved query model failed: " + e, e, true);
        }

        if (attempt.queryFactory != null) {
            log.info("Persisted query model restored in {} ms (model {})", (System.nanoTime() - start) / 1_000_000L,
                    persistedQueryModel.getName());
            return attempt.queryFactory;
        }
        return fallback(attempt.reason, null, attempt.warn);
    }

    private Attempt restore() throws Exception {
        final String name = persistedQueryModel.getName();
        final QueryModelPersistence.LoadResult loaded;
        try (InputStream stream = persistedQueryModel.openStream()) {
            if (stream == null) {
                return Attempt.unusable("the saved query model is missing (no stream of " + name + "-query.model)", false);
            }
            loaded = QueryModelPersistence.load(name, stream, asmModel.getResource(), expressionModel.getResource());
        }

        final List<String> reasons = new ArrayList<>();
        if (loaded.getUnresolvedProxyCount() > 0) {
            reasons.add("unresolved proxies: " + loaded.getUnresolvedProxyCount() + " (" + loaded.getUnresolvedProxySamples() + ")");
        }
        for (String mismatch : QueryModelPersistence.checkStamps(loaded.getInfo(), asmModel.getName(), asmDigest, measureDigest, expressionDigest)) {
            reasons.add(mismatch(mismatch));
        }
        if (!reasons.isEmpty()) {
            return Attempt.unusable(String.join("; ", reasons), true);
        }

        return Attempt.restored(QueryFactory.restore(asmModel, measureModel, loaded.getQueryModel()));
    }

    /**
     * {@code asmDigest: artifact A, expected B} to {@code asmDigest mismatch: artifact A, expected B}.
     */
    private static String mismatch(final String stampMessage) {
        final int colon = stampMessage.indexOf(": ");
        return colon < 0 ? stampMessage : stampMessage.substring(0, colon) + " mismatch" + stampMessage.substring(colon);
    }

    private QueryFactory fallback(final String reason, final Throwable cause, final boolean warn) {
        if (mode == Mode.LOAD) {
            throw new IllegalStateException("Query model mode is load, but " + reason, cause);
        }
        final String model = persistedQueryModel != null ? persistedQueryModel.getName() : asmModelName();
        if (warn) {
            if (cause != null) {
                log.warn("Persisted query model of {} is not used, the query model is built: {}", model, reason, cause);
            } else {
                log.warn("Persisted query model of {} is not used, the query model is built: {}", model, reason);
            }
        } else {
            log.info("Persisted query model of {} is not used, the query model is built: {}", model, reason);
        }
        return buildQueryFactory.get();
    }

    private String asmModelName() {
        try {
            return asmModel.getName();
        } catch (RuntimeException e) {
            return "<unknown>";
        }
    }

    private static final class Attempt {
        private final QueryFactory queryFactory;
        private final String reason;
        private final boolean warn;

        private Attempt(final QueryFactory queryFactory, final String reason, final boolean warn) {
            this.queryFactory = queryFactory;
            this.reason = reason;
            this.warn = warn;
        }

        static Attempt restored(final QueryFactory queryFactory) {
            return new Attempt(queryFactory, null, false);
        }

        static Attempt unusable(final String reason, final boolean warn) {
            return new Attempt(null, reason, warn);
        }
    }
}
