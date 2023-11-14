package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

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
import com.google.inject.name.Named;
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

import javax.annotation.Nullable;

@SuppressWarnings("rawtypes")
public class RdbmsBuilderProvider implements Provider<RdbmsBuilder> {


    public static final String RDBMS_DAO_FLOATING_POINT_TYPE_MAX_PRECISION = "rdbmsDaoFloatingTypeMaxPrecision";
    public static final String RDBMS_DAO_FLOATING_POINT_TYPE_MAX_SCALE = "rdbmsDaoFloatingTypeMaxScale";

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

    @Inject(optional = true)
    @Named(RDBMS_DAO_FLOATING_POINT_TYPE_MAX_PRECISION)
    @Nullable
    private int precision = 15;

    @Inject(optional = true)
    @Named(RDBMS_DAO_FLOATING_POINT_TYPE_MAX_SCALE)
    @Nullable
    private int scale = 4;

    @SuppressWarnings({ "unchecked" })
    @Override
    public RdbmsBuilder get() {
        AsmUtils asm = new AsmUtils(asmModel.getResourceSet());

        return RdbmsBuilder.builder()
                .rdbmsModel(rdbmsModel)
                .ancestorNameFactory(new AncestorNameFactory(asm.all(EClass.class)))
                .descendantNameFactory(new DescendantNameFactory(asm.all(EClass.class)))
                .rdbmsResolver(rdbmsResolver)
                .parameterMapper(rdbmsParameterMapper)
                .asmUtils(asm)
                .identifierProvider(identifierProvider)
                .coercer(coercer)
                .variableResolver(variableResolver)
                .mapperFactory(mapperFactory)
                .dialect(dialect)
                .precision(precision)
                .scale(scale)
                .build();
    }
}
