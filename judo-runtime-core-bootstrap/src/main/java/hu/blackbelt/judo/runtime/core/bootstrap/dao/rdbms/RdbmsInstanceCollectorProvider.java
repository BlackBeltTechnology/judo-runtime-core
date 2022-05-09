package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

public class RdbmsInstanceCollectorProvider implements Provider<InstanceCollector> {

    @Inject
    DataSource dataSource;

    @Inject
    RdbmsResolver rdbmsResolver;

    @Inject
    JudoModelSpecification models;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    Dialect dialect;

    @Override
    public InstanceCollector get() {
        InstanceCollector instanceCollector = RdbmsInstanceCollector.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .asmUtils(new AsmUtils(models.getAsmModel().getResourceSet()))
                .rdbmsResolver(rdbmsResolver)
                .rdbmsModel(models.getRdbmsModel())
                .coercer(dataTypeManager.getCoercer())
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .identifierProvider(identifierProvider)
                .dialect(dialect)
                .build();
        return instanceCollector;
    }
}
