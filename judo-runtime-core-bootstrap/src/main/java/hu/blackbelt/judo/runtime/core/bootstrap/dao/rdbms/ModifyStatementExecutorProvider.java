package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;

import javax.inject.Provider;

@SuppressWarnings("rawtypes")
public class ModifyStatementExecutorProvider implements Provider<ModifyStatementExecutor> {

    @Inject
    AsmModel asmModel;

    @Inject
    RdbmsModel rdbmsModel;

    @Inject
    Coercer coercer;

	@Inject
    IdentifierProvider identifierProvider;

    @Inject
    TransformationTraceService transformationTraceService;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    RdbmsResolver rdbmsResolver;

    @SuppressWarnings("unchecked")
	@Override
    public ModifyStatementExecutor get() {
        return ModifyStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .transformationTraceService(this.transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .coercer(coercer)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }
}
