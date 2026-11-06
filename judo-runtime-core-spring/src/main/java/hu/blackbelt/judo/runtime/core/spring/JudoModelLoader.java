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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsDataTypes.support.RdbmsDataTypesModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsNameMapping.support.RdbmsNameMappingModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.support.RdbmsTableMappingRulesModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.tatami.asm2query.PersistedQueryModel;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.LoadArguments.asmLoadArgumentsBuilder;

@Builder
@Getter
@Slf4j
public class JudoModelLoader {

    @NonNull
    AsmModel asmModel;

    @NonNull
    RdbmsModel rdbmsModel;

    @NonNull
    MeasureModel measureModel;

    @NonNull
    ExpressionModel expressionModel;

    @NonNull
    LiquibaseModel liquibaseModel;

    @NonNull
    Asm2RdbmsTransformationTrace asm2rdbms;

    /** SHA-256 of the complete bytes of the {@code -asm.model} file, {@code null} when not loaded from files. */
    String asmDigest;

    /** SHA-256 of the complete bytes of the {@code -measure.model} file, {@code null} when not loaded from files. */
    String measureDigest;

    /** SHA-256 of the complete bytes of the {@code -expression.model} file, {@code null} when not loaded from files. */
    String expressionDigest;

    /** Locator of {@code <name>-query.model}; {@code null} when no such file exists. Never opened by the loader. */
    PersistedQueryModel persistedQueryModel;

    public static JudoModelLoader loadFromClassloader(String modelName, ClassLoader classLoader, Dialect dialect, boolean validate) throws Exception {

        Enumeration<URL> urlEnumeration = classLoader.getResources("model");
        URL url = null;
        while (urlEnumeration.hasMoreElements() && url == null) {
            URL urlToTest = urlEnumeration.nextElement();
            try {
                URL relativeUrl = calculateRelativeURI(urlToTest.toURI(), "/" + modelName + "-asm.model").toURL();
                InputStream stream = relativeUrl.openStream();
                if (stream != null) {
                    url = urlToTest;
                    try {
                        stream.close();
                    } catch (Exception e2) {
                    }
                }
            } catch (Exception e) {
            }
        }

        if (url == null) {
            throw new FileNotFoundException(String.format("The model \"%s\" could not be found (possible file name and model name mismatch)!", modelName));
        }

        return loadFromURL(modelName, url.toURI(), dialect, validate);
    }

    public static JudoModelLoader loadFromDirectory(String modelName, File directory, Dialect dialect) throws Exception {
        if (directory == null) {
            throw new IllegalArgumentException("Directory is null");
        }
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exitsts: " + directory);
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Given file is not directory: " + directory);
        }

        return loadFromURL(modelName, directory.toURI(), dialect, true);
    }

    public static JudoModelLoader loadFromURL(String modelName, URI uri, Dialect dialect) throws Exception {
        return loadFromURL(modelName, uri, dialect, true);
    }

    public static JudoModelLoader loadFromURL(String modelName, URI uri, Dialect dialect, boolean validate) throws Exception {

        if (modelName == null) {
            throw new IllegalArgumentException("Model name have to be defined");
        }
        if (uri == null) {
            throw new IllegalArgumentException("URI name have to be defined");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("Dialect name have to be defined");
        }

        final AsmModel asmModel;
        final String asmDigest;
        try (DigestedStream in = DigestedStream.open(calculateRelativeURI(uri, "/" + modelName + "-asm.model"))) {
            asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                    .inputStream(in.stream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(modelName + "-asm.model"))
                    .validateModel(validate));
            asmDigest = in.digest();
        }

        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();

        // The RDBMS model resources have to know the mapping models
        RdbmsNameMappingModelResourceSupport.registerRdbmsNameMappingMetamodel(rdbmsModel.getResourceSet());
        RdbmsDataTypesModelResourceSupport.registerRdbmsDataTypesMetamodel(rdbmsModel.getResourceSet());
        RdbmsTableMappingRulesModelResourceSupport.registerRdbmsTableMappingRulesMetamodel(rdbmsModel.getResourceSet());

        RdbmsModel.loadRdbmsModel(RdbmsModel.LoadArguments.rdbmsLoadArgumentsBuilder()
                .resourceSet(rdbmsModel.getResourceSet())
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-rdbms_" + dialect.getName() +".model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-rdbms_" + dialect.getName() + ".model"))
                .validateModel(validate));

        final MeasureModel measureModel;
        final String measureDigest;
        try (DigestedStream in = DigestedStream.open(calculateRelativeURI(uri, "/" + modelName + "-measure.model"))) {
            measureModel = MeasureModel.loadMeasureModel(MeasureModel.LoadArguments.measureLoadArgumentsBuilder()
                    .inputStream(in.stream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-measure.model"))
                    .validateModel(validate)
                    .name(asmModel.getName()));
            measureDigest = in.digest();
        }

        final ExpressionModel expressionModel;
        final String expressionDigest;
        try (DigestedStream in = DigestedStream.open(calculateRelativeURI(uri, "/" + modelName + "-expression.model"))) {
            expressionModel = ExpressionModel.loadExpressionModel(ExpressionModel.LoadArguments.expressionLoadArgumentsBuilder()
                    .inputStream(in.stream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-expression.model"))
                    .validateModel(validate)
                    .name(asmModel.getName()));
            expressionDigest = in.digest();
        }

        LiquibaseModel liquibaseModel = LiquibaseModel.loadLiquibaseModel(LiquibaseModel.LoadArguments.liquibaseLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-liquibase_" + dialect.getName() + ".changelog.xml").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-liquibase_" + dialect.getName() + ".changelog.xml"))
                .validateModel(validate)
                .name(asmModel.getName()));

        Asm2RdbmsTransformationTrace asm2rdbms  = Asm2RdbmsTransformationTrace.fromModelsAndTrace(modelName,
                asmModel, rdbmsModel,
                calculateRelativeURI(uri, "/" + modelName + "-asm2rdbms_" + dialect.getName() + ".model").toURL().openStream());


        // the query model is never read here: only a locator is built, the decision opens it when it needs it
        final URI queryModelUri = calculateRelativeURI(uri, "/" + modelName + QueryModelPersistence.QUERY_SUFFIX);
        final PersistedQueryModel persistedQueryModel = exists(queryModelUri)
                ? new PersistedQueryModel(modelName, () -> queryModelUri.toURL().openStream())
                : null;

        return JudoModelLoader.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .liquibaseModel(liquibaseModel)
                .asm2rdbms(asm2rdbms)
                .asmDigest(asmDigest)
                .measureDigest(measureDigest)
                .expressionDigest(expressionDigest)
                .persistedQueryModel(persistedQueryModel)
                .build();
    }

    /**
     * Existence check that does not read the file: file system lookup for {@code file:}, jar entry lookup for
     * {@code jar:}, an opened-and-closed stream for any other protocol.
     */
    static boolean exists(final URI uri) {
        try {
            final URL url = uri.toURL();
            if ("file".equals(url.getProtocol())) {
                return new File(uri).isFile();
            }
            final URLConnection connection = url.openConnection();
            if (connection instanceof JarURLConnection) {
                try {
                    return ((JarURLConnection) connection).getJarEntry() != null;
                } catch (FileNotFoundException e) {
                    return false;
                }
            }
            try (InputStream ignored = connection.getInputStream()) {
                return true;
            }
        } catch (IOException | RuntimeException e) {
            log.debug("No query model at {}: {}", uri, e.toString());
            return false;
        }
    }

    /**
     * A model file stream that digests every byte. The model loaders may stop reading before EOF, so
     * {@link #digest()} drains the rest of the file first: the digest always covers the complete file.
     * The stream handed to the loader ignores {@code close()}; it is closed with this object.
     */
    private static final class DigestedStream implements AutoCloseable {
        private final QueryModelPersistence.DigestInputStream digestStream;
        private final InputStream shield;

        private DigestedStream(final InputStream in) {
            this.digestStream = new QueryModelPersistence.DigestInputStream(in);
            this.shield = new FilterInputStream(digestStream) {
                @Override
                public void close() {
                    // closed by DigestedStream.close(), after the digest was taken
                }
            };
        }

        static DigestedStream open(final URI uri) throws IOException {
            return new DigestedStream(uri.toURL().openStream());
        }

        InputStream stream() {
            return shield;
        }

        String digest() throws IOException {
            return digestStream.digest();
        }

        @Override
        public void close() throws IOException {
            digestStream.close();
        }
    }


    @SneakyThrows(URISyntaxException.class)
    private static URI calculateRelativeURI(URI base, String path) {
        //URI root = JudoModelHolder.class.getProtectionDomain().getCodeSource().getLocation().toURI();
        String root = base.toString();
        if (root.endsWith("/")) {
            root.substring(0, root.length() - 1);
        }
        String rel = path;
        if (rel.startsWith("/")) {
            rel = path.substring(1);
        }
        URI ret = base;
        if (root.endsWith(".jar")) {
            ret = new URI("jar:" + root.toString() + "!/" + rel);
        } else if (root.startsWith("jar:bundle:")) {
            ret = new URI(root.substring(4, root.indexOf("!")) + rel);
        } else {
            ret = new URI(root + "/" + rel);
        }
        return ret;
    }
}
