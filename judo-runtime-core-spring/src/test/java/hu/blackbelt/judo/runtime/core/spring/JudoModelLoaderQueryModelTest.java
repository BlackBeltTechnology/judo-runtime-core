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

import hu.blackbelt.judo.runtime.core.spring.ModelSources.Source;
import hu.blackbelt.judo.runtime.core.spring.ModelSources.State;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Task 1.6: the Spring loader digests the ASM, measure and expression model files and locates the query model without
 * reading it, for a directory, a classpath jar and a file URL.
 */
class JudoModelLoaderQueryModelTest {

    @AfterAll
    static void cleanUp() {
        ModelSources.cleanUp();
    }

    @TestFactory
    List<DynamicTest> digestsAndLocatorForEverySourceAndState() {
        final NorthwindModelSet models = NorthwindModelSet.shared();
        final List<DynamicTest> tests = new ArrayList<>();
        for (Source source : Source.values()) {
            for (State state : State.values()) {
                tests.add(DynamicTest.dynamicTest(source + " / " + state, () -> {
                    final JudoModelLoader loader = ModelSources.load(source, state);

                    // digests: SHA-256 of the complete bytes of each file as read (the stale state changed the expression file)
                    assertThat(loader.getAsmDigest(), equalTo(models.digestOf("-asm.model")));
                    assertThat(loader.getMeasureDigest(), equalTo(models.digestOf("-measure.model")));
                    if (state == State.STALE) {
                        assertThat("the digest follows the changed file", loader.getExpressionDigest(),
                                org.hamcrest.Matchers.not(equalTo(models.digestOf("-expression.model"))));
                    } else {
                        assertThat(loader.getExpressionDigest(), equalTo(models.digestOf("-expression.model")));
                    }

                    // locator: present when the file exists - a corrupt file is still located, because it is never read here
                    final PersistedQueryModel locator = loader.getPersistedQueryModel();
                    if (state == State.ABSENT) {
                        assertThat("no query file, no locator", locator, nullValue());
                    } else {
                        assertThat(locator, notNullValue());
                        assertThat(locator.getName(), equalTo(models.name));
                    }
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> locatorStreamIsTheQueryModelFile() {
        final NorthwindModelSet models = NorthwindModelSet.shared();
        final List<DynamicTest> tests = new ArrayList<>();
        for (Source source : Source.values()) {
            tests.add(DynamicTest.dynamicTest(source.toString(), () -> {
                final JudoModelLoader loader = ModelSources.load(source, State.PRESENT);
                try (java.io.InputStream in = loader.getPersistedQueryModel().openStream()) {
                    assertThat(java.util.Arrays.equals(in.readAllBytes(), models.queryModel), equalTo(true));
                }
            }));
        }
        return tests;
    }
}
