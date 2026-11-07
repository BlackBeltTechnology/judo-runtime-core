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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseNamespaceFixUriHandler;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.asm2expression.Asm2ExpressionWork;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsWork;
import hu.blackbelt.judo.tatami.core.workflow.work.TransformationContext;
import hu.blackbelt.judo.tatami.rdbms2liquibase.Rdbms2LiquibaseWork;
import hu.blackbelt.model.northwind.Demo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static hu.blackbelt.judo.tatami.psm2asm.etl.Psm2Asm.Psm2AsmParameter.psm2AsmParameter;
import static hu.blackbelt.judo.tatami.psm2asm.etl.Psm2Asm.executePsm2AsmTransformation;
import static hu.blackbelt.judo.tatami.psm2measure.etl.Psm2Measure.Psm2MeasureParameter.psm2MeasureParameter;
import static hu.blackbelt.judo.tatami.psm2measure.etl.Psm2Measure.executePsm2MeasureTransformation;

/**
 * Test helper. The complete model file set of Northwind for the {@code hsqldb} dialect, generated once per JVM with the
 * judo-tatami-base transformations, plus the persisted query model saved from the same live models in the same run.
 * Files are exactly the bytes the runtime model loaders read (and digest).
 */
public final class NorthwindModelSet {

    public static final String DIALECT = "hsqldb";

    public final String name;
    /** Model files by file name, the query model excluded. */
    public final Map<String, byte[]> files = new LinkedHashMap<>();
    public final byte[] queryModel;
    public final String queryModelFileName;

    private NorthwindModelSet() throws Exception {
        final PsmModel psmModel = new Demo().fullDemo();
        final AsmModel asmModel = AsmModel.buildAsmModel().build();
        final MeasureModel measureModel = MeasureModel.buildMeasureModel().name(psmModel.getName()).build();
        executePsm2AsmTransformation(psm2AsmParameter().psmModel(psmModel).asmModel(asmModel).parallel(true).useCache(true));
        executePsm2MeasureTransformation(psm2MeasureParameter().psmModel(psmModel).measureModel(measureModel).parallel(true).useCache(true));

        final TransformationContext context = new TransformationContext(psmModel.getName());
        context.put(asmModel);
        context.put(measureModel);
        new Asm2ExpressionWork(context).execute();
        final ExpressionModel expressionModel = context.getByClass(ExpressionModel.class).orElseThrow(IllegalStateException::new);
        new Asm2RdbmsWork(context, DIALECT).execute();
        new Rdbms2LiquibaseWork(context, DIALECT).execute();

        this.name = psmModel.getName();
        final RdbmsModel rdbmsModel = Asm2RdbmsWork.getRdbmsModel(context, DIALECT).orElseThrow(IllegalStateException::new);
        final LiquibaseModel liquibaseModel = Rdbms2LiquibaseWork.getLiquibaseModel(context, DIALECT).orElseThrow(IllegalStateException::new);

        files.put(name + "-asm.model", bytes(out -> asmModel.saveAsmModel(AsmModel.SaveArguments.asmSaveArgumentsBuilder().outputStream(out))));
        files.put(name + "-measure.model", bytes(out -> measureModel.saveMeasureModel(MeasureModel.SaveArguments.measureSaveArgumentsBuilder().outputStream(out))));
        files.put(name + "-expression.model", bytes(out -> expressionModel.saveExpressionModel(ExpressionModel.SaveArguments.expressionSaveArgumentsBuilder().outputStream(out))));
        files.put(name + "-rdbms_" + DIALECT + ".model", bytes(out -> rdbmsModel.saveRdbmsModel(RdbmsModel.SaveArguments.rdbmsSaveArgumentsBuilder().validateModel(false).outputStream(out))));
        files.put(name + "-liquibase_" + DIALECT + ".changelog.xml", bytes(out -> liquibaseModel.saveLiquibaseModel(LiquibaseModel.SaveArguments.liquibaseSaveArgumentsBuilder()
                .validateModel(false)
                .outputStream(LiquibaseNamespaceFixUriHandler.fixUriOutputStream(out)))));
        final File trace = File.createTempFile(name + "-asm2rdbms", ".model");
        try {
            Asm2RdbmsWork.getAsm2RdbmsTrace(context, DIALECT).orElseThrow(IllegalStateException::new).save(trace);
            files.put(name + "-asm2rdbms_" + DIALECT + ".model", Files.readAllBytes(trace.toPath()));
        } finally {
            trace.delete();
        }

        // the persisted query model: built from the same live models, stamped with the digests of the files above
        final QueryFactory built = QueryFactory.builder()
                .asmResourceSet(asmModel.getResourceSet())
                .measureResourceSet(measureModel.getResourceSet())
                .expressionResourceSet(expressionModel.getResourceSet())
                .customJoinDefinitions(new HashMap<>())
                .build();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final QueryModelPersistence.SaveResult result = QueryModelPersistence.save(QueryModelPersistence.SaveArguments.saveArguments()
                .queryModel(built.getQueryModel())
                .asmModel(asmModel)
                .expressionModel(expressionModel)
                .asmDigest(QueryModelPersistence.sha256Hex(files.get(name + "-asm.model")))
                .measureDigest(QueryModelPersistence.sha256Hex(files.get(name + "-measure.model")))
                .expressionDigest(QueryModelPersistence.sha256Hex(files.get(name + "-expression.model")))
                .outputStream(out)
                .build());
        if (!result.isWritten()) {
            throw new IllegalStateException("The query model was not saved: " + result.getProblems());
        }
        this.queryModel = out.toByteArray();
        this.queryModelFileName = name + QueryModelPersistence.QUERY_SUFFIX;
    }

    private interface Saver {
        void save(OutputStream out) throws Exception;
    }

    private static byte[] bytes(final Saver saver) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        saver.save(out);
        return out.toByteArray();
    }

    private static NorthwindModelSet shared;

    public static synchronized NorthwindModelSet shared() {
        if (shared == null) {
            try {
                shared = new NorthwindModelSet();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return shared;
    }

    /** Writes the model files into the directory, and the query model when requested. */
    public void writeDirectory(final File directory, final boolean withQueryModel) throws IOException {
        Files.createDirectories(directory.toPath());
        for (Map.Entry<String, byte[]> file : files.entrySet()) {
            Files.write(new File(directory, file.getKey()).toPath(), file.getValue());
        }
        if (withQueryModel) {
            Files.write(new File(directory, queryModelFileName).toPath(), queryModel);
        }
    }

    public String digestOf(final String suffix) {
        return QueryModelPersistence.sha256Hex(files.get(name + suffix));
    }
}
