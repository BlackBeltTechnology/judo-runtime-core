package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsSequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.HsqldbRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers.HsqldbMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.mapper.api.Coercer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

@Configuration
@ConditionalOnExpression("'${spring.datasource.url}'.contains('hsqldb')")
public class JudoHsqldbSpringConfiguration {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private TransactionManager transactionManager;

    @Bean
    public HsqldbDialect getHsqlsbDialect() {
        return new HsqldbDialect();
    }

    @Bean
    public Sequence getHsqlsbSequence() {
        // TODO: Paramters
        Long start = 1L;
        Long increment = 1L;
        Boolean createIfNotExists = true;

        HsqldbRdbmsSequence.HsqldbRdbmsSequenceBuilder builder = HsqldbRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public RdbmsParameterMapper getHsqlsbRdbmsParameterMapper(
            IdentifierProvider identifierProvider,
            Dialect dialect,
            Coercer coercer,
            RdbmsModel rdbmsModel
    ) {
        return HsqldbRdbmsParameterMapper.builder()
                .coercer(coercer)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .build();
    }

    @Bean
    public MapperFactory getHsqlsbMapperFactory() {
        return new HsqldbMapperFactory();
    }

}
