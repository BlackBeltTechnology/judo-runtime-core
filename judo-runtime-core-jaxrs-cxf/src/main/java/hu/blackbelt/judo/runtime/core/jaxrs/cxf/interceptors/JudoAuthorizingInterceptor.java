package hu.blackbelt.judo.runtime.core.jaxrs.cxf.interceptors;

/*-
 * #%L
 * JUDO Services Security for CXF
 * %%
 * Copyright (C) 2018 - 2023 BlackBelt Technology
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

import hu.blackbelt.judo.dispatcher.api.JudoOperation;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.NonNull;
import org.apache.cxf.interceptor.security.AbstractAuthorizingInInterceptor;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EOperation;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JudoAuthorizingInterceptor extends AbstractAuthorizingInInterceptor {

    private AsmModel asmModel;

    @Builder
    public JudoAuthorizingInterceptor(@NonNull AsmModel asmModel) {
        this.asmModel = asmModel;
    }

    @Override
    protected List<String> getExpectedRoles(final Method method) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        Objects.requireNonNull(method, "Method is mandatory");

        final String operationFQName = method.getDeclaredAnnotation(JudoOperation.class).value();

        final EOperation operation = asmUtils.resolveOperation(operationFQName)
                .orElseThrow(() -> new IllegalStateException("Operation not found in ASM model: " + operationFQName));

        final List<EClassifier> accessPoints = AsmUtils.getExtensionAnnotationListByName(operation, "exposedBy").stream()
                .map(a -> a.getDetails().get("value"))
                .filter(accessPointFqName -> accessPointFqName != null)
                .map(accessPointFqName -> asmUtils.resolve(accessPointFqName).orElseThrow(() -> new IllegalStateException("Access point not found: " + accessPointFqName)))
                .collect(Collectors.toList());

        final boolean publicActor = accessPoints.stream()
                .map(accessPoint -> AsmUtils.getExtensionAnnotationByName(accessPoint, "actor", false).orElse(null))
                .anyMatch(actor -> actor == null || !actor.getDetails().containsKey("realm") || "".equals(actor.getDetails().get("realm")));

        return publicActor ? Collections.emptyList() : accessPoints.stream().map(accessPoint -> AsmUtils.getClassifierFQName(accessPoint)).collect(Collectors.toList());
    }
}
