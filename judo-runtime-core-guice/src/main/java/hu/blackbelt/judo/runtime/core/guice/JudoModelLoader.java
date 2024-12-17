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

import hu.blackbelt.epsilon.runtime.execution.impl.BufferedSlf4jLogger;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.support.AsmModelResourceSupport;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.liquibase.support.LiquibaseModelResourceSupport;
import hu.blackbelt.judo.meta.liquibase.util.builder.databaseChangeLogBuilder;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.meta.keycloak.support.KeycloakModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsDataTypes.support.RdbmsDataTypesModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsNameMapping.support.RdbmsNameMappingModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.support.RdbmsTableMappingRulesModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.asm2keycloak.Asm2KeycloakTransformationTrace;

import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.util.builder.EPackageBuilder;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.LoadArguments.asmLoadArgumentsBuilder;
import static hu.blackbelt.judo.tatami.asm2rdbms.ExcelMappingModels2Rdbms.*;

@Getter
@Slf4j
public class JudoModelLoader {

    AsmModel asmModel;
    RdbmsModel rdbmsModel;
    MeasureModel measureModel;
    ExpressionModel expressionModel;
    LiquibaseModel liquibaseModel;
    KeycloakModel keycloakModel;
    Asm2RdbmsTransformationTrace asm2rdbms;
    Asm2KeycloakTransformationTrace asm2keycloak;
    Boolean useKeycloak;

    public static class JudoModelLoaderBuilder {
        AsmModel asmModel;
        RdbmsModel rdbmsModel;
        MeasureModel measureModel;
        ExpressionModel expressionModel;
        LiquibaseModel liquibaseModel;
        KeycloakModel keycloakModel;
        Asm2RdbmsTransformationTrace asm2rdbms;
        Asm2KeycloakTransformationTrace asm2keycloak;
        Boolean useKeycloak = true;
    }

    @Builder
    public JudoModelLoader(AsmModel asmModel,
                            RdbmsModel rdbmsModel,
                            MeasureModel measureModel,
                            ExpressionModel expressionModel,
                            LiquibaseModel liquibaseModel,
                            KeycloakModel keycloakModel,
                            Asm2RdbmsTransformationTrace asm2rdbms,
                            Asm2KeycloakTransformationTrace asm2keycloak,
                            Boolean useKeycloak) {
        this.asmModel = asmModel;
        this.rdbmsModel = rdbmsModel;
        this.measureModel = measureModel;
        this.expressionModel = expressionModel;
        this.liquibaseModel = liquibaseModel;
        this.keycloakModel = keycloakModel;
        this.asm2rdbms = asm2rdbms;
        this.asm2keycloak = asm2keycloak;
        this.useKeycloak = useKeycloak;
    }

    public static JudoModelLoader loadFromClassloader(String modelName, ClassLoader classLoader, Dialect dialect, boolean validate, boolean loadKeycloak) throws Exception {
        URL url = null;
        ClassPathResource resource = new ClassPathResource("model/" + modelName + "-asm.model", classLoader);
        List<URL> classPathUrls = new ArrayList<>();
        URL urlToTest = resource.getURL();
        Collection<FileSystem> openedFilesSystems = new ArrayList<>();
        classPathUrls.add(urlToTest);
        if (resource.exists()) {
            try {
                url = Paths.get(resource.getURL().toURI()).getParent().toUri().toURL();
            } catch (FileSystemNotFoundException e) {
                // in this case we need to initialize it first:
                for (FileSystemProvider provider: FileSystemProvider.installedProviders()) {
                    if (provider.getScheme().equalsIgnoreCase("jar")) {
                        try {
                            provider.getFileSystem(urlToTest.toURI());
                        } catch (FileSystemNotFoundException e2) {
                            // in this case we need to initialize it first:
                            openedFilesSystems.add(provider.newFileSystem(urlToTest.toURI(), Collections.emptyMap()));
                        }
                    }
                }
                url = Paths.get(resource.getURL().toURI()).getParent().toUri().toURL();
            }
        }
        if (url == null) {
            throw new IllegalArgumentException("Could not load model from classpath: " + modelName + " from: \n" + classPathUrls.stream().map(u -> u.toString()).collect(Collectors.joining("\n\t")));
        } else {
            log.info("Model loaded from: " + url.toString());
        }
        JudoModelLoader modelLoader = loadFromURL(modelName, url.toURI(), dialect, validate, loadKeycloak);
        openedFilesSystems.forEach(fs -> {
                if (fs.isOpen()) {
                    try {
                        fs.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        });
        return modelLoader;
    }

    public static JudoModelLoader loadFromDirectory(String modelName, File directory, Dialect dialect, boolean loadKeycloak) throws Exception {
        return loadFromDirectory(modelName, directory, dialect, true, loadKeycloak);
    }

    public static JudoModelLoader loadFromDirectory(String modelName, File directory, Dialect dialect, boolean validate, boolean loadKeycloak) throws Exception {
        if (directory == null) {
            throw new IllegalArgumentException("Directory is null");
        }
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory does not exitsts: " + directory);
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("Given file is not directory: " + directory);
        }

        return loadFromURL(modelName, directory.toURI(), dialect, validate, loadKeycloak);
    }

    public static JudoModelLoader loadFromURL(String modelName, URI uri, Dialect dialect, boolean loadKeycloak) throws Exception {
        return loadFromURL(modelName, uri, dialect, true, loadKeycloak);
    }

    public static JudoModelLoader loadFromURL(String modelName, URI uri, Dialect dialect, boolean validate, boolean loadKeycloak) throws Exception {

        if (modelName == null) {
            throw new IllegalArgumentException("Model name have to be defined");
        }
        if (uri == null) {
            throw new IllegalArgumentException("URI name have to be defined");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("Dialect name have to be defined");
        }

        AsmModel asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-asm.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelName + "-asm.model"))
                .validateModel(validate));

        if (!asmModel.getName().equals(modelName)) {
            throw new IllegalArgumentException("Model name does not match with ASM Model: " + modelName + " ASM: " + asmModel.getName());
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

        MeasureModel measureModel = MeasureModel.loadMeasureModel(MeasureModel.LoadArguments.measureLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-measure.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-measure.model"))
                .validateModel(validate)
                .name(asmModel.getName()));

        ExpressionModel expressionModel = ExpressionModel.loadExpressionModel(ExpressionModel.LoadArguments.expressionLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-expression.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-expression.model"))
                .validateModel(validate)
                .name(asmModel.getName()));

        LiquibaseModel liquibaseModel = LiquibaseModel.loadLiquibaseModel(LiquibaseModel.LoadArguments.liquibaseLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-liquibase_" + dialect.getName() + ".changelog.xml").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-liquibase_" + dialect.getName() + ".changelog.xml"))
                .validateModel(validate)
                .name(asmModel.getName()));

        Asm2RdbmsTransformationTrace asm2rdbms  = Asm2RdbmsTransformationTrace.fromModelsAndTrace(modelName,
                asmModel, rdbmsModel,
                calculateRelativeURI(uri, "/" + modelName + "-asm2rdbms_" + dialect.getName() + ".model").toURL().openStream());

        KeycloakModel keycloakModel = null;
        Asm2KeycloakTransformationTrace asm2keycloak = null;
        if (loadKeycloak) {
            keycloakModel = KeycloakModel.loadKeycloakModel(KeycloakModel.LoadArguments.keycloakLoadArgumentsBuilder()
                    .inputStream(calculateRelativeURI(uri, "/" + modelName + "-liquibase_" + dialect.getName() + ".changelog.xml").toURL().openStream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(asmModel.getName() + "-liquibase_" + dialect.getName() + ".changelog.xml"))
                    .validateModel(validate)
                    .name(asmModel.getName()));

            asm2keycloak = Asm2KeycloakTransformationTrace.fromModelsAndTrace(modelName,
                    asmModel, keycloakModel,
                    calculateRelativeURI(uri, "/" + modelName + "-asm2keycloak.model").toURL().openStream());
        }

        return JudoModelLoader.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .liquibaseModel(liquibaseModel)
                .keycloakModel(keycloakModel)
                .asm2rdbms(asm2rdbms)
                .asm2keycloak(asm2keycloak)
                .build();
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


    @Builder
    public static JudoModelLoader load(String modelName, File directory, Dialect dialect) throws Exception {
        return loadFromDirectory(modelName, directory, dialect, true);
    }


    public static JudoModelLoader empty() throws Exception {

        AsmModel asmModel = AsmModel.buildAsmModel()
                .resourceSet(AsmModelResourceSupport.createAsmResourceSet())
                .build();

        asmModel.getAsmModelResourceSupport().addContent(EPackageBuilder.create()
                .withName("judo").withNsPrefix("judo").withNsURI("http://blackbelt.hu/test/judo/judo").build());

        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();

        // The RDBMS model resources have to know the mapping models
        RdbmsNameMappingModelResourceSupport.registerRdbmsNameMappingMetamodel(rdbmsModel.getResourceSet());
        RdbmsDataTypesModelResourceSupport.registerRdbmsDataTypesMetamodel(rdbmsModel.getResourceSet());
        RdbmsTableMappingRulesModelResourceSupport.registerRdbmsTableMappingRulesMetamodel(rdbmsModel.getResourceSet());
        try (BufferedSlf4jLogger bufferedLog = new BufferedSlf4jLogger(log)) {
            injectExcelMappings(rdbmsModel, bufferedLog, calculateExcelMapping2RdbmsTransformationScriptURI(), calculateExcelMappingModelURI(), "hsqldb");
        }

        MeasureModel measureModel = MeasureModel.buildMeasureModel()
                .name(asmModel.getName())
                .resourceSet(MeasureModelResourceSupport.createMeasureResourceSet())
                .build();

        ExpressionModel expressionModel = ExpressionModel.buildExpressionModel()
                .name(asmModel.getName())
                .resourceSet(ExpressionModelResourceSupport.createExpressionResourceSet())
                .build();

        LiquibaseModel liquibaseModel = LiquibaseModel.buildLiquibaseModel()
                .name(asmModel.getName())
                .resourceSet(LiquibaseModelResourceSupport.createLiquibaseResourceSet())
                .build();

        liquibaseModel.getResource().getContents().add(databaseChangeLogBuilder.create().build());

        KeycloakModel keycloakModel = KeycloakModel.buildKeycloakModel()
                .name(asmModel.getName())
                .resourceSet(KeycloakModelResourceSupport.createKeycloakResourceSet())
                .build();

        Asm2RdbmsTransformationTrace asm2rdbms = Asm2RdbmsTransformationTrace.asm2RdbmsTransformationTraceBuilder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .trace(new HashMap<>())
                .build();

        Asm2KeycloakTransformationTrace asm2keycloak = Asm2KeycloakTransformationTrace.asm2KeycloakTransformationTraceBuilder()
                .asmModel(asmModel)
                .keycloakModel(keycloakModel)
                .trace(new HashMap<>())
                .build();

        JudoModelLoader judoModelLoader = JudoModelLoader.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .liquibaseModel(liquibaseModel)
                .keycloakModel(keycloakModel)
                .asm2rdbms(asm2rdbms)
                .asm2keycloak(asm2keycloak)
                .build();
        return judoModelLoader;
    }
}
