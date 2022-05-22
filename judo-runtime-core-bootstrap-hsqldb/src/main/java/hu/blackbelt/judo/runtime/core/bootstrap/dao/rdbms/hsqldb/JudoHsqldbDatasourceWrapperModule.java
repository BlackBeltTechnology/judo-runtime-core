package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;


import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import com.google.inject.util.Providers;
import org.hsqldb.server.Server;

import com.google.inject.Binder;

public class JudoHsqldbDatasourceWrapperModule implements com.google.inject.Module {
	DataSource datasource;
	TransactionManager transactionManager;
	
	public JudoHsqldbDatasourceWrapperModule(DataSource datasource, TransactionManager transactionManager) {
		this.datasource = datasource;		
		this.transactionManager = transactionManager;
	}
	@Override
	public void configure(Binder binder) {
		binder.bind(Server.class).toProvider(Providers.of(null));
        binder.bind(DataSource.class).toInstance(datasource);
        binder.bind(TransactionManager.class).toInstance(transactionManager);		
	}
}
