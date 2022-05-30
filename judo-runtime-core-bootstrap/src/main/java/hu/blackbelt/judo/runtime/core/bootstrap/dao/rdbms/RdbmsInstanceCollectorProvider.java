package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.mapper.api.Coercer;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@SuppressWarnings("rawtypes")
public class RdbmsInstanceCollectorProvider implements Provider<InstanceCollector> {

    @Inject
    DataSource dataSource;

    @Inject
    RdbmsResolver rdbmsResolver;

    @Inject
    AsmModel asmModel;

    @Inject
    RdbmsModel rdbmsModel;

    @Inject
    Coercer coercer;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @SuppressWarnings("unchecked")
	@Override
    public InstanceCollector get() {
        InstanceCollector instanceCollector = RdbmsInstanceCollector.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .asmUtils(new AsmUtils(asmModel.getResourceSet()))
                .rdbmsResolver(rdbmsResolver)
                .rdbmsModel(rdbmsModel)
                .coercer(coercer)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .identifierProvider(identifierProvider)
                .build();
        return instanceCollector;
    }
}
