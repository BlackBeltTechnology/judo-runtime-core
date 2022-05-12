package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

import org.eclipse.emf.ecore.EClass;

@SuppressWarnings("rawtypes")
public class RdbmsBuilderProvider implements Provider<RdbmsBuilder> {
    @Inject
    JudoModelSpecification models;

    @Inject
    RdbmsResolver rdbmsResolver;

	@Inject
    RdbmsParameterMapper rdbmsParameterMapper;

	@Inject
    IdentifierProvider identifierProvider;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    VariableResolver variableResolver;
    
	@Inject
    MapperFactory mapperFactory;

    @Inject
    Dialect dialect;

    @SuppressWarnings({ "unchecked" })
	@Override
    public RdbmsBuilder get() {
        AsmUtils asm = new AsmUtils(models.getAsmModel().getResourceSet());

        return RdbmsBuilder.builder()
                .rdbmsModel(models.getRdbmsModel())
                .ancestorNameFactory(new AncestorNameFactory(asm.all(EClass.class)))
                .rdbmsResolver(rdbmsResolver)
                .parameterMapper(rdbmsParameterMapper)
                .asmUtils(asm)
                .identifierProvider(identifierProvider)
                .coercer(dataTypeManager.getCoercer())
                .variableResolver(variableResolver)
                .mapperFactory(mapperFactory)
                .dialect(dialect)
                .build();
    }
}
