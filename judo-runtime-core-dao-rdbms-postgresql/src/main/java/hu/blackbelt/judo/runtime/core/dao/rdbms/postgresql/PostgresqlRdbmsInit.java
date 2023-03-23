package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql;

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

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import lombok.Builder;
import lombok.NonNull;

import javax.sql.DataSource;

public class PostgresqlRdbmsInit implements RdbmsInit {
    private final SimpleLiquibaseExecutor liquibaseExecutor;

    private final LiquibaseModel liquibaseModel;

    @Builder
    public PostgresqlRdbmsInit(@NonNull SimpleLiquibaseExecutor liquibaseExecutor, @NonNull LiquibaseModel liquibaseModel) {
        this.liquibaseExecutor = liquibaseExecutor;
        this.liquibaseModel = liquibaseModel;
    }

    public void execute(DataSource dataSource) {
        liquibaseExecutor.executeInitiLiquibase(PostgresqlRdbmsInit.class.getClassLoader(), "liquibase/postgresql-init-changelog.xml", dataSource);
        liquibaseExecutor.createDatabase(dataSource, liquibaseModel);
    }

}
