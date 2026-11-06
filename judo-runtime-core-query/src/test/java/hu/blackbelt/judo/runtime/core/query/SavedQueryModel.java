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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A built query factory over the Northwind models, with its saved artifact (the way the model build persists it), the
 * file digests of the models it was built from, and helpers to make the artifact stale or broken.
 *
 * <p>The query model is built first, and the digests are taken after it: the build attaches the iterators of collection
 * expressions to the expression model (historic behaviour), so the expression file is saved after the build.</p>
 */
final class SavedQueryModel {

    final NorthwindFixture fixture;
    final QueryFactory built;
    final String name;
    final byte[] asmBytes;
    final byte[] measureBytes;
    final byte[] expressionBytes;
    final String asmDigest;
    final String measureDigest;
    final String expressionDigest;
    final byte[] artifact;

    SavedQueryModel(final NorthwindFixture fixture) {
        try {
            this.fixture = fixture;
            this.built = QueryFactory.builder()
                    .asmResourceSet(fixture.asmModel.getResourceSet())
                    .measureResourceSet(fixture.measureModel.getResourceSet())
                    .expressionResourceSet(fixture.expressionModel.getResourceSet())
                    .customJoinDefinitions(new HashMap<>())
                    .build();
            this.name = built.getQueryModel().getName();

            this.asmBytes = bytes(out -> fixture.asmModel.saveAsmModel(AsmModel.SaveArguments.asmSaveArgumentsBuilder().outputStream(out)));
            this.measureBytes = bytes(out -> fixture.measureModel.saveMeasureModel(MeasureModel.SaveArguments.measureSaveArgumentsBuilder().outputStream(out)));
            this.expressionBytes = bytes(out -> fixture.expressionModel.saveExpressionModel(ExpressionModel.SaveArguments.expressionSaveArgumentsBuilder().outputStream(out)));
            this.asmDigest = QueryModelPersistence.sha256Hex(asmBytes);
            this.measureDigest = QueryModelPersistence.sha256Hex(measureBytes);
            this.expressionDigest = QueryModelPersistence.sha256Hex(expressionBytes);

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final QueryModelPersistence.SaveResult result = QueryModelPersistence.save(QueryModelPersistence.SaveArguments.saveArguments()
                    .queryModel(built.getQueryModel())
                    .asmModel(fixture.asmModel)
                    .expressionModel(fixture.expressionModel)
                    .asmDigest(asmDigest)
                    .measureDigest(measureDigest)
                    .expressionDigest(expressionDigest)
                    .outputStream(out)
                    .build());
            if (!result.isWritten()) {
                throw new IllegalStateException("The query model was not saved: " + result.getProblems());
            }
            this.artifact = out.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private interface Saver {
        void save(ByteArrayOutputStream out) throws Exception;
    }

    private static byte[] bytes(final Saver saver) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        saver.save(out);
        return out.toByteArray();
    }

    private static SavedQueryModel shared;

    /**
     * Read-only users share one instance; a test that changes a model or counts builds creates its own.
     */
    static synchronized SavedQueryModel shared() {
        if (shared == null) {
            shared = new SavedQueryModel(NorthwindFixture.shared());
        }
        return shared;
    }

    /**
     * A locator over bytes; {@code opened} counts the calls of its stream supplier.
     */
    PersistedQueryModel locator(final byte[] bytes, final AtomicInteger opened) {
        return new PersistedQueryModel(name, () -> {
            opened.incrementAndGet();
            return new ByteArrayInputStream(bytes);
        });
    }

    /**
     * A locator whose stream supplier is the given one; {@code opened} counts its calls.
     */
    PersistedQueryModel locator(final PersistedQueryModel.StreamSupplier supplier, final AtomicInteger opened) {
        return new PersistedQueryModel(name, () -> {
            opened.incrementAndGet();
            return supplier.open();
        });
    }

    /**
     * The artifact with the value of one attribute of the {@code QueryModelInfo} root replaced.
     */
    byte[] withInfoAttribute(final String attribute, final String value) {
        final String xml = new String(artifact, java.nio.charset.StandardCharsets.UTF_8);
        final java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(<query:QueryModelInfo\\b[^>]*?\\b" + attribute + "=\")[^\"]*(\")").matcher(xml);
        if (!matcher.find()) {
            throw new IllegalStateException("Attribute " + attribute + " not found in the QueryModelInfo of the artifact");
        }
        return (xml.substring(0, matcher.start()) + matcher.group(1) + value + matcher.group(2) + xml.substring(matcher.end()))
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * The decision over this artifact with every input matching: digests, models, no custom joins, the shared build.
     */
    QueryModelLoadOrBuild.QueryModelLoadOrBuildBuilder decision(final PersistedQueryModel locator) {
        return QueryModelLoadOrBuild.queryModelLoadOrBuild()
                .asmModel(fixture.asmModel).asmDigest(asmDigest)
                .measureModel(fixture.measureModel).measureDigest(measureDigest)
                .expressionModel(fixture.expressionModel).expressionDigest(expressionDigest)
                .persistedQueryModel(locator);
    }

    /**
     * Load the artifact into the live models, without a decision.
     */
    QueryModelPersistence.LoadResult load() throws IOException {
        return QueryModelPersistence.load(name, new ByteArrayInputStream(artifact),
                fixture.asmModel.getResource(), fixture.expressionModel.getResource());
    }
}
