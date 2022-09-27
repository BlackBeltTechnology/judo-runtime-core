package hu.blackbelt.judo.runtime.core.security;

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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.NonNull;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

public class PathInfoRealmExtractor implements RealmExtractor {

    private EList<EClass> actors;


    @Builder
    public PathInfoRealmExtractor(@NonNull  AsmModel asmModel) {
        final AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        actors = asmUtils.getAllActorTypes();
    }

    @Override
    public Optional<EClass> extractActorType(final HttpServletRequest httpServletRequest) {
        final String pathInfo = httpServletRequest.getPathInfo();

        if (pathInfo == null) {
            return Optional.empty();
        }
        return actors.stream()
                .filter(actor -> pathInfo.startsWith("/" + AsmUtils.getClassifierFQName(actor).replace(".", "/")))
                .findAny();
    }
}
