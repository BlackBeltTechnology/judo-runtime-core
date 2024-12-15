package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.postgresql;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.utils.RuntimeVariableResolver;
import lombok.*;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class JudoPostgresqlModuleConfiguration {
    public final static JudoPostgresqlModuleConfiguration DEFAULT = JudoPostgresqlModuleConfiguration.builder().build();
    @Builder.Default
    RuntimeVariableResolver runtimeVariableResolver = null;
    @Builder.Default
    String host = "localhost";
    @Builder.Default
    Integer port = 5432;
    @Builder.Default
    String user = "judo";
    @Builder.Default
    String password = "judo";
    @Builder.Default
    String databaseName = "judo";
    @Builder.Default
    Integer poolSize = 10;
    @Builder.Default
    PlatformTransactionManager platformTransactionManager = null;
    @Builder.Default
    MapperFactory mapperFactory = null;
    @Builder.Default
    RdbmsParameterMapper rdbmsParameterMapper = null;
    @Builder.Default
    DataSource dataSource = null;
    @Builder.Default
    Sequence sequence = null;
    @Builder.Default
    RdbmsInit rdbmsInit = null;
}
