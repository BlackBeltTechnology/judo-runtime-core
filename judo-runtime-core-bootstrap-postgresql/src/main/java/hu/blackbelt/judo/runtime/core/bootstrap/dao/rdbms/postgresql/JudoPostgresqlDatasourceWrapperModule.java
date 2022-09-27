package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

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
