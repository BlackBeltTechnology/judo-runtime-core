package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultIdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;

import javax.annotation.Nullable;

@SuppressWarnings("rawtypes")
public class DefaultIdentifierSignerProvider implements Provider<IdentifierSigner> {

    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";

    @Inject
    AsmModel asmModel;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject(optional = true)
    @Named(IDENTIFIER_SIGNER_SECRET)
    @Nullable
    String secret;

    @Override
    @SuppressWarnings("unchecked")
    public IdentifierSigner get() {

        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(secret)
                .build();
    }
}
