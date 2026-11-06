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

import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test helper. Materializes the Northwind model set in a directory, in a jar on a classpath or behind a file URL, with
 * the query model in one of the states the runtime has to cope with, and loads it with the Spring {@link JudoModelLoader}.
 */
final class ModelSources {

    enum Source {
        DIRECTORY, CLASSPATH, URL
    }

    enum State {
        /** valid query model */
        PRESENT,
        /** no query model file */
        ABSENT,
        /** query model file truncated at 50% */
        CORRUPT,
        /** valid query model, but the expression model file changed (one byte appended) after the save */
        STALE
    }

    private static final List<Path> TEMPORARY = new ArrayList<>();
    private static final List<URLClassLoader> CLASS_LOADERS = new ArrayList<>();

    private ModelSources() {
    }

    /** Closes the class loaders and deletes the temporary files of every {@link #load} so far. */
    static void cleanUp() {
        synchronized (CLASS_LOADERS) {
            for (URLClassLoader classLoader : CLASS_LOADERS) {
                try {
                    classLoader.close();
                } catch (IOException e) {
                    // best effort: temporary files only
                }
            }
            CLASS_LOADERS.clear();
        }
        synchronized (TEMPORARY) {
            for (Path root : TEMPORARY) {
                try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
                    paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                } catch (IOException e) {
                    // best effort: temporary files only
                }
            }
            TEMPORARY.clear();
        }
    }

    static JudoModelLoader load(final Source source, final State state) throws Exception {
        return load(source, state, NorthwindModelSet.shared());
    }

    static JudoModelLoader load(final Source source, final State state, final NorthwindModelSet models) throws Exception {
        final Path root = Files.createTempDirectory("judo-query-model-" + source + "-" + state);
        synchronized (TEMPORARY) {
            TEMPORARY.add(root);
        }
        final File directory = new File(root.toFile(), "model");
        write(models, directory, state);

        switch (source) {
            case DIRECTORY:
                return JudoModelLoader.loadFromDirectory(models.name, directory, new HsqldbDialect());
            case URL:
                // a file URL without the trailing slash of File.toURI() on a directory
                final String uri = directory.toURI().toString();
                return JudoModelLoader.loadFromURL(models.name, URI.create(uri.substring(0, uri.length() - 1)), new HsqldbDialect(), true);
            case CLASSPATH:
                final File jar = new File(root.toFile(), models.name + ".jar");
                packIntoJar(directory, jar);
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{jar.toURI().toURL()}, ClassLoader.getPlatformClassLoader());
                synchronized (CLASS_LOADERS) {
                    CLASS_LOADERS.add(classLoader);
                }
                return JudoModelLoader.loadFromClassloader(models.name, classLoader, new HsqldbDialect(), true);
            default:
                throw new IllegalArgumentException(String.valueOf(source));
        }
    }

    /** Writes the model files and the query model of the state into the directory. */
    static void write(final NorthwindModelSet models, final File directory, final State state) throws IOException {
        models.writeDirectory(directory, state != State.ABSENT);
        switch (state) {
            case CORRUPT:
                final File query = new File(directory, models.queryModelFileName);
                Files.write(query.toPath(), java.util.Arrays.copyOf(models.queryModel, models.queryModel.length / 2));
                break;
            case STALE:
                final File expression = new File(directory, models.name + "-expression.model");
                Files.write(expression.toPath(), (new String(Files.readAllBytes(expression.toPath()), java.nio.charset.StandardCharsets.UTF_8) + "\n")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                break;
            default:
                break;
        }
    }

    /** Packs the files of a directory (as they are, corrupt or stale included) under {@code model/} of the jar. */
    private static void packIntoJar(final File directory, final File jar) throws IOException {
        try (java.util.jar.JarOutputStream out = new java.util.jar.JarOutputStream(new java.io.FileOutputStream(jar))) {
            out.putNextEntry(new java.util.jar.JarEntry("model/"));
            out.closeEntry();
            for (File file : directory.listFiles()) {
                out.putNextEntry(new java.util.jar.JarEntry("model/" + file.getName()));
                out.write(Files.readAllBytes(file.toPath()));
                out.closeEntry();
            }
        }
    }
}
