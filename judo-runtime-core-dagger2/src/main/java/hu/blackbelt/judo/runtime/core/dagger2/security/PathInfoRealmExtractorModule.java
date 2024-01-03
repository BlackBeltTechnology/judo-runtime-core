package hu.blackbelt.judo.runtime.core.dagger2.security;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.security.PathInfoRealmExtractor;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;

import javax.inject.Inject;

@Module
public class PathInfoRealmExtractorModule {
    @JudoApplicationScope
    @Provides
    public RealmExtractor providesRealmExtractor(AsmModel asmModel) {
        return new PathInfoRealmExtractor(asmModel);
    }
}
