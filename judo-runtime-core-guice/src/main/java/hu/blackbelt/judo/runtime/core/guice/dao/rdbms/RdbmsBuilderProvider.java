package hu.blackbelt.judo.runtime.core.guice.dao.rdbms;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.DescendantNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;

import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;

@SuppressWarnings("rawtypes")
public class RdbmsBuilderProvider implements Provider<RdbmsBuilder> {

    @Inject
    AsmModel asmModel;

    @Inject
    RdbmsModel rdbmsModel;

    @Inject
    RdbmsResolver rdbmsResolver;

    @Inject
    RdbmsParameterMapper rdbmsParameterMapper;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    Coercer coercer;

    @Inject
    VariableResolver variableResolver;

    @Inject
    MapperFactory mapperFactory;

    @Inject
    Dialect dialect;

    @SuppressWarnings({ "unchecked" })
    @Override
    public RdbmsBuilder get() {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        return RdbmsBuilder.builder()
                .rdbmsModel(rdbmsModel)
                .ancestorNameFactory(new AncestorNameFactory(asmUtils.all(EClass.class)))
                .descendantNameFactory(new DescendantNameFactory(asmUtils.all(EClass.class)))
                .rdbmsResolver(rdbmsResolver)
                .parameterMapper(rdbmsParameterMapper)
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .coercer(coercer)
                .variableResolver(variableResolver)
                .mapperFactory(mapperFactory)
                .dialect(dialect)
                .build();
    }
}
