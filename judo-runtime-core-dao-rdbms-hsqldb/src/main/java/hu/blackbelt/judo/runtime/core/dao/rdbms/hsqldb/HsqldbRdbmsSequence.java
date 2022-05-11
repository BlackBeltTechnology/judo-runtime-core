package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb;

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Collections;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNullElse;

public class HsqldbRdbmsSequence implements Sequence<Long> {

    private static final Long DEFAULT_START = Sequence.DEFAULT_START;
    private static final long DEFAULT_INCREMENT = Sequence.DEFAULT_INCREMENT;

    private final DataSource dataSource;
    private final Long start;
    private final Long increment;
    private final Boolean createIfNotExists;


    @Builder
    public HsqldbRdbmsSequence(@NonNull DataSource dataSource,
                                   Long start, Long increment,
                                   Boolean createIfNotExists) {
        this.dataSource = dataSource;
        this.start = requireNonNullElse(start, DEFAULT_START);
        this.increment = requireNonNullElse(increment, DEFAULT_INCREMENT);
        this.createIfNotExists = requireNonNullElse(createIfNotExists, true);
    }

    @AllArgsConstructor
    private enum Operation {
        CURRENT_VALUE("CURRVAL"), NEXT_VALUE("NEXTVAL");
        String functionName;
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

    @Override
    public Long getNextValue(final String sequenceName) {
        return execute(sequenceName, Operation.NEXT_VALUE);
    }

    @Override
    public Long getCurrentValue(String sequenceName) {
        return execute(sequenceName, Operation.CURRENT_VALUE);
    }
}
