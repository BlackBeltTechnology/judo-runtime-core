package hu.blackbelt.judo.runtime.core.guice.dao.rdbms.hsqldb;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import lombok.*;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class JudoHsqldbModuleConfiguration {
    public static final JudoHsqldbModuleConfiguration DEFAULT = JudoHsqldbModuleConfiguration.builder().build();
    @Builder.Default
    private Boolean runServer = false;
    @Builder.Default
    private String databaseName = "judo";
    @Builder.Default
    private File databasePath = new File(".", "judo.db");
    @Builder.Default
    private Integer port = 31001;
    @Builder.Default
    private PlatformTransactionManager platformTransactionManager = null;
    @Builder.Default
    private MapperFactory mapperFactory = null;
    @Builder.Default
    private RdbmsParameterMapper rdbmsParameterMapper = null;
    @Builder.Default
    private DataSource dataSource = null;
    @Builder.Default
    private Sequence sequence = null;
    @Builder.Default
    private RdbmsInit rdbmsInit = null;
}
