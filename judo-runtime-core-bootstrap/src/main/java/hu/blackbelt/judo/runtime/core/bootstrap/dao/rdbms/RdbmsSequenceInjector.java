package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import hu.blackbelt.judo.runtime.core.dao.rdbms.sequence.RdbmsSequence;

import javax.sql.DataSource;

public class RdbmsSequenceInjector extends RdbmsSequence {
    @Inject
    public void setDataSource(DataSource dataSource) {
        super.setDataSource(dataSource);
    }

}
