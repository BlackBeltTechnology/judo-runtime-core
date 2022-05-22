package hu.blackbelt.judo.runtime.core.dao.rdbms;

import javax.sql.DataSource;

public interface RdbmsInit {
    void execute(DataSource dataSource);
}
