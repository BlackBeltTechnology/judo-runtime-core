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
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;

import static hu.blackbelt.judo.meta.asm.runtime.AsmModel.LoadArguments.asmLoadArgumentsBuilder;

@Builder
@Getter
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

        AsmModel asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-asm.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelName + "-asm.model"))
                .validateModel(validate)
                .name(modelName));

        String modelNameFromAsm;

        if (asmModel.getResourceSet().getResources().get(0).getAllContents().hasNext()) {
            EObject o = asmModel.getResourceSet().getResources().get(0).getAllContents().next();
            if (o instanceof EPackage) {
                EPackage ePackage = (EPackage) o;
                modelNameFromAsm = ePackage.getName();
            } else {
                throw new IllegalStateException("Illegal ASM model, root package must be EPackage");
            }
        } else {
            throw new IllegalStateException("Illegal ASM model, empty model");
        }

        if (!modelNameFromAsm.equals(modelName)) {
            asmModel = AsmModel.loadAsmModel(asmLoadArgumentsBuilder()
                    .inputStream(calculateRelativeURI(uri, "/" + modelName + "-asm.model").toURL().openStream())
                    .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm + "-asm.model"))
                    .validateModel(validate)
                    .name(modelNameFromAsm));
        }


        RdbmsModel rdbmsModel = RdbmsModel.buildRdbmsModel()
                .name(modelNameFromAsm)
                .resourceSet(RdbmsModelResourceSupport.createRdbmsResourceSet())
                .build();

        // The RDBMS model resources have to know the mapping models
        RdbmsNameMappingModelResourceSupport.registerRdbmsNameMappingMetamodel(rdbmsModel.getResourceSet());
        RdbmsDataTypesModelResourceSupport.registerRdbmsDataTypesMetamodel(rdbmsModel.getResourceSet());
        RdbmsTableMappingRulesModelResourceSupport.registerRdbmsTableMappingRulesMetamodel(rdbmsModel.getResourceSet());

        RdbmsModel.loadRdbmsModel(RdbmsModel.LoadArguments.rdbmsLoadArgumentsBuilder()
                .resourceSet(rdbmsModel.getResourceSet())
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-rdbms_" + dialect.getName() +".model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-rdbms_" + dialect.getName() + ".model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        MeasureModel measureModel = MeasureModel.loadMeasureModel(MeasureModel.LoadArguments.measureLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-measure.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-measure.model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        ExpressionModel expressionModel = ExpressionModel.loadExpressionModel(ExpressionModel.LoadArguments.expressionLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-expression.model").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-expression.model"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        LiquibaseModel liquibaseModel = LiquibaseModel.loadLiquibaseModel(LiquibaseModel.LoadArguments.liquibaseLoadArgumentsBuilder()
                .inputStream(calculateRelativeURI(uri, "/" + modelName + "-liquibase_" + dialect.getName() + ".changelog.xml").toURL().openStream())
                .uri(org.eclipse.emf.common.util.URI.createURI(modelNameFromAsm+"-liquibase_" + dialect.getName() + ".changelog.xml"))
                .validateModel(validate)
                .name(modelNameFromAsm));

        Asm2RdbmsTransformationTrace asm2rdbms  = Asm2RdbmsTransformationTrace.fromModelsAndTrace(modelName,
                asmModel, rdbmsModel,
                calculateRelativeURI(uri, "/" + modelName + "-asm2rdbms_" + dialect.getName() + ".model").toURL().openStream());


        return JudoModelLoader.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .measureModel(measureModel)
                .expressionModel(expressionModel)
                .liquibaseModel(liquibaseModel)
                .asm2rdbms(asm2rdbms)
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
}
