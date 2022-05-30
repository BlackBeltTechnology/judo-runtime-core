package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.query.mappers.PostgresqlMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.sequence.PostgresqlRdbmsSequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.mapper.api.Coercer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

@Configuration
@ConditionalOnExpression("'${spring.datasource.url}'.contains('postgresql')")
public class JudoPostgresqlSpringConfiguration {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private TransactionManager transactionManager;

    @Bean
    public PostgresqlDialect getPostgresqlDialect() {
        return new PostgresqlDialect();
    }

    @Bean
    public Sequence getPostgresqlSequence() {
        // TODO: Paramters
        Long start = 1L;
        Long increment = 1L;
        Boolean createIfNotExists = true;

        PostgresqlRdbmsSequence.PostgresqlRdbmsSequenceBuilder builder = PostgresqlRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public RdbmsParameterMapper getPostgresqlRdbmsParameterMapper(
            IdentifierProvider identifierProvider,
            Dialect dialect,
            Coercer coercer,
            RdbmsModel rdbmsModel
    ) {
        return PostgresqlRdbmsParameterMapper.builder()
                .coercer(coercer)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .build();
    }

    @Bean
    public MapperFactory getPostgresqlMapperFactory() {
        return new PostgresqlMapperFactory();
    }

}
