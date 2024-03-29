package hu.blackbelt.judo.runtime.core.spring.hsqldb;

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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsSequence;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.HsqldbRdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.query.mappers.HsqldbMapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.mapper.api.Coercer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@ConditionalOnExpression("'${spring.datasource.url}'.contains('hsqldb')")
public class JudoHsqldbSpringConfiguration {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public HsqldbDialect getHsqlsbDialect() {
        return new HsqldbDialect();
    }

    @Bean
    public Sequence getHsqlsbSequence() {
        // TODO: Paramters
        Long start = 1L;
        Long increment = 1L;
        Boolean createIfNotExists = true;

        HsqldbRdbmsSequence.HsqldbRdbmsSequenceBuilder builder = HsqldbRdbmsSequence.builder();
        builder
                .dataSource(dataSource)
                .start(start)
                .increment(increment)
                .createIfNotExists(createIfNotExists);

        return builder.build();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public RdbmsParameterMapper getHsqlsbRdbmsParameterMapper(
            IdentifierProvider identifierProvider,
            Dialect dialect,
            Coercer coercer,
            RdbmsModel rdbmsModel
    ) {
        return HsqldbRdbmsParameterMapper.builder()
                .coercer(coercer)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .build();
    }

    @Bean
    public MapperFactory getHsqlsbMapperFactory() {
        return new HsqldbMapperFactory();
    }

}
