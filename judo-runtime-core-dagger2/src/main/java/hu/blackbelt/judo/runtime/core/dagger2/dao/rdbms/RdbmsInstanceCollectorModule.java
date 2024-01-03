package hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.mapper.api.Coercer;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

import javax.inject.Inject;

@SuppressWarnings("rawtypes")
@Module
public class RdbmsInstanceCollectorModule {

    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public InstanceCollector providesInstanceCollector(
            DataSource dataSource,
            RdbmsResolver rdbmsResolver,
            AsmModel asmModel,
            RdbmsModel rdbmsModel,
            Coercer coercer,
            IdentifierProvider identifierProvider,
            RdbmsParameterMapper rdbmsParameterMapper) {
        InstanceCollector instanceCollector = RdbmsInstanceCollector.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .asmModel(asmModel)
                .rdbmsResolver(rdbmsResolver)
                .rdbmsModel(rdbmsModel)
                .coercer(coercer)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .identifierProvider(identifierProvider)
                .build();
        return instanceCollector;
    }
}
