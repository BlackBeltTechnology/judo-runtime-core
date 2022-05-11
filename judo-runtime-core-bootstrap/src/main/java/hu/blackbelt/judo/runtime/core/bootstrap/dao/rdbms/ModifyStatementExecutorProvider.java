package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

import javax.inject.Provider;

public class ModifyStatementExecutorProvider implements Provider<ModifyStatementExecutor> {

    @Inject
    JudoModelSpecification models;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    RdbmsResolver rdbmsResolver;

    @Override
    public ModifyStatementExecutor get() {
        return ModifyStatementExecutor.builder()
                .asmModel(models.getAsmModel())
                .rdbmsModel(models.getRdbmsModel())
                .identifierProvider(identifierProvider)
                .transformationTraceService(this.transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .coercer(dataTypeManager.getCoercer())
                .rdbmsResolver(rdbmsResolver)
                .build();
    }
}
