package hu.blackbelt.judo.runtime.core.dao.rdbms.sequence;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkState;

@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class RdbmsSequence implements Sequence<Long> {

    static final String TYPE = "rdbms";

    private static final Long DEFAULT_START = Sequence.DEFAULT_START;
    private static final long DEFAULT_INCREMENT = Sequence.DEFAULT_INCREMENT;


    @AllArgsConstructor
    private enum Operation {

        CURRENT_VALUE("CURRVAL"), NEXT_VALUE("NEXTVAL");

        String functionName;
    }

    @NonNull
    DataSource dataSource;

    private Long start = DEFAULT_START;

    private Long increment = DEFAULT_INCREMENT;

    private boolean createIfNotExists = true;

    private Dialect dialect;

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
