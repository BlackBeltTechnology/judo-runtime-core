package hu.blackbelt.judo.runtime.core.bootstrap.security;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.security.PathInfoRealmExtractor;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;

public class PathInfoRealmExtractorProvider implements Provider<RealmExtractor> {

    AsmModel asmModel;

    @Inject
    public PathInfoRealmExtractorProvider(AsmModel asmModel) {
       this. asmModel = asmModel;
    }

    @Override
    public RealmExtractor get() {
        return new PathInfoRealmExtractor(asmModel);
    }
}
