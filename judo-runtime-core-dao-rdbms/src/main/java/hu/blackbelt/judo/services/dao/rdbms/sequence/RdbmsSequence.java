package hu.blackbelt.judo.services.dao.rdbms.sequence;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.services.dao.rdbms.Dialect;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkState;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        Sequence.TYPE_KEY + "=" + RdbmsSequence.TYPE
})
@Designate(ocd = RdbmsSequence.Config.class)
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class RdbmsSequence implements Sequence<Long> {

    static final String TYPE = "rdbms";

    private static final Long DEFAULT_START = Sequence.DEFAULT_START;
    private static final long DEFAULT_INCREMENT = Sequence.DEFAULT_INCREMENT;

    @ObjectClassDefinition()
    public @interface Config {

        @AttributeDefinition(name = "sqlDialect", description = "SQL dialect", defaultValue = "hsqldb")
        String sqlDialect();

        @AttributeDefinition(name = "jooqEnabled", description = "Enable JOOQ", type = AttributeType.BOOLEAN)
        boolean jooq_enabled();

        @AttributeDefinition(name = "start", description = "Start value", type = AttributeType.LONG)
        long start() default 1L;

        @AttributeDefinition(name = "increment", description = "Increment by", type = AttributeType.LONG)
        long increment() default 1L;

        @AttributeDefinition(name = "createIfNotExists", description = "Create sequence if not exists yet", type = AttributeType.BOOLEAN)
        boolean createIfNotExists() default true;
    }

    @AllArgsConstructor
    private enum Operation {

        CURRENT_VALUE("CURRVAL"), NEXT_VALUE("NEXTVAL");

        String functionName;
    }

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    @NonNull
    DataSource dataSource;

    private Long start = DEFAULT_START;

    private Long increment = DEFAULT_INCREMENT;

    private boolean createIfNotExists = true;

    private Dialect dialect;

    @Activate
    void start(Config config) {
        start = config.start();
        increment = config.increment();
        createIfNotExists = config.createIfNotExists();
        dialect = Dialect.parse(config.sqlDialect(), config.jooq_enabled());
    }

    private Long execute(String sequenceName, final Operation operation) {
        sequenceName = sequenceName.replaceAll("[^a-zA-Z0-9_]", "_");
        final NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        if (createIfNotExists) {
            jdbcTemplate.execute("CREATE SEQUENCE IF NOT EXISTS " + sequenceName +
                            (start != null ? " START WITH " + start : "") +
                            (increment != null ? " INCREMENT BY " + increment : ""),
                    Collections.emptyMap(),
                    (stmt) -> stmt.execute());
        }
        switch (dialect) {
            case HSQLDB: {
                final String sql;
                switch (operation) {
                    case NEXT_VALUE: {
                        sql = "CALL NEXT VALUE FOR " + sequenceName;
                        break;
                    }
                    case CURRENT_VALUE: {
                        sql = "CALL CURRENT VALUE FOR " + sequenceName;
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unsupported sequence operation");
                }
                return jdbcTemplate.execute(sql, prst -> {
                    checkState(prst.execute(), "Unable to call sequence");
                    final ResultSet rs = prst.getResultSet();
                    checkState(rs.next(), "Unable to get sequence value");
                    return rs.getLong(1);
                });
            }
            case POSTGRESQL:
                return jdbcTemplate.queryForObject("SELECT " + (operation.functionName) + "('" + sequenceName + "')", Collections.emptyMap(), Long.class);
            case JOOQ:
                return jdbcTemplate.queryForObject("SELECT " + (operation.functionName) + "(" + sequenceName + ")", Collections.emptyMap(), Long.class);
        }
        throw new UnsupportedOperationException("Sequence is not supported in dialect: " + dialect);
    }

    @Override
    public Long getNextValue(final String sequenceName) {
        return execute(sequenceName, Operation.NEXT_VALUE);
    }

    @Override
    public Long getCurrentValue(String sequenceName) {
        return execute(sequenceName, Operation.CURRENT_VALUE);
    }
}
