package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

public class GetTemplateCall<ID> implements BehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;

    final OperationCallInterceptorProvider interceptorProvider;

    final AsmModel asmModel;

    public GetTemplateCall(DAO<ID> dao, AsmModel asmModel, OperationCallInterceptorProvider interceptorProvider) {
        this.dao = dao;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.interceptorProvider = interceptorProvider;
        this.asmModel = asmModel;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_TEMPLATE).isPresent();
    }

    @Override
    public Object call(final Map<String, Object> exchange, final EOperation operation) {
        final EClass owner = (EClass) CallInterceptorUtil.preCallInterceptors(asmModel, operation, interceptorProvider,
                asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation).orElseThrow(
                        () -> new IllegalArgumentException("Invalid model")));
        Payload result = null;
        if (CallInterceptorUtil.isOriginalCalled(asmModel, operation, interceptorProvider)) {
            result = dao.getDefaultsOf(owner);
        }
        return CallInterceptorUtil.postCallInterceptors(asmModel, operation, interceptorProvider, owner, result);
    }
}
