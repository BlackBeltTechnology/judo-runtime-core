package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;


import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import com.google.inject.Binder;

public class JudoPostgresqlDatasourceWrapperModule implements com.google.inject.Module {
	DataSource datasource;
	TransactionManager transactionManager;
	
	public JudoPostgresqlDatasourceWrapperModule(DataSource datasource, TransactionManager transactionManager) {
		this.datasource = datasource;		
		this.transactionManager = transactionManager;
	}
	@Override
	public void configure(Binder binder) {
        binder.bind(DataSource.class).toInstance(datasource);
        binder.bind(TransactionManager.class).toInstance(transactionManager);		
	}
}
