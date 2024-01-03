package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

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
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultIdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

import static java.util.Objects.requireNonNullElse;

@SuppressWarnings("rawtypes")
@Module
public class DefaultIdentifierSignerModule {

    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";


    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public IdentifierSigner providesIdentifierSigner(
            AsmModel asmModel,
            DataTypeManager dataTypeManager,
            IdentifierProvider identifierProvider,
            @Named(IDENTIFIER_SIGNER_SECRET) @Nullable String secret
    ) {

        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(requireNonNullElse(secret, generateNewSecret()))
                .build();
    }

    private String generateNewSecret() {
        final SecureRandom random;
        try {
            random = SecureRandom.getInstanceStrong();
            final byte[] values = new byte[1024 / 8];
            random.nextBytes(values);
            return Base64.getEncoder().encodeToString(values);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
