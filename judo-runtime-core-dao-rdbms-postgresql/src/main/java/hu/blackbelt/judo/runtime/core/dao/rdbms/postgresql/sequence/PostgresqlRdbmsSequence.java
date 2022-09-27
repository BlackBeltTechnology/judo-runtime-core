package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.sequence;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dispatcher.api.Sequence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.Collections;

import static java.util.Objects.requireNonNullElse;

@Builder
public class PostgresqlRdbmsSequence implements Sequence<Long> {

    private static final Long DEFAULT_START = Sequence.DEFAULT_START;
    private static final long DEFAULT_INCREMENT = Sequence.DEFAULT_INCREMENT;

    private final DataSource dataSource;
    private final Long start;
    private final Long increment;
    private final Boolean createIfNotExists;


    @Builder
    public PostgresqlRdbmsSequence(@NonNull DataSource dataSource,
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
        return jdbcTemplate.queryForObject("SELECT " + (operation.functionName) + "('" + sequenceName + "')", Collections.emptyMap(), Long.class);
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
