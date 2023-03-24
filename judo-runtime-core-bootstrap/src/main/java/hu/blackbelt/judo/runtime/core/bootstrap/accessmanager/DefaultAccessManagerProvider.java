package hu.blackbelt.judo.runtime.core.bootstrap.accessmanager;

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
import hu.blackbelt.judo.runtime.core.accessmanager.DefaultAccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;

public class DefaultAccessManagerProvider implements Provider<AccessManager> {

    AsmModel asmModel;

    @Inject
    public DefaultAccessManagerProvider(AsmModel asmModel) {
        this.asmModel = asmModel;
    }


    @Override
    public AccessManager get() {
        return DefaultAccessManager.builder()
                .asmModel(asmModel)
                .build();
    }
}
