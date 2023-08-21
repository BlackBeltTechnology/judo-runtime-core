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
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class GetPrincipalCall<ID> implements BehaviourCall<ID> {

    final AsmModel asmModel;

    final AsmUtils asmUtils;
    final DAO<ID> dao;
    final ActorResolver actorResolver;
    final IdentifierProvider<ID> identifierProvider;

    final OperationCallInterceptorProvider interceptorProvider;
    public GetPrincipalCall(DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                            ActorResolver actorResolver, OperationCallInterceptorProvider interceptorProvider) {
        this.dao = dao;
        this.asmModel = asmModel;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.actorResolver = actorResolver;
        this.interceptorProvider = interceptorProvider;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_PRINCIPAL).isPresent();
    }

    @Override
    public Object call(final Map<String, Object> exchange, final EOperation operation) {
        Map<String, Object> inputParameter = (Map<String, Object>) CallInterceptorUtil.preCallInterceptors(asmModel, operation, interceptorProvider, exchange);
        Payload ret = null;
        if (CallInterceptorUtil.isOriginalCalled(asmModel, operation, interceptorProvider)) {
            final Optional<Payload> actor;
            if (inputParameter.containsKey(Dispatcher.ACTOR_KEY)) {
                actor = Optional.ofNullable((Payload) inputParameter.get(Dispatcher.ACTOR_KEY));
            } else if (inputParameter.containsKey(Dispatcher.PRINCIPAL_KEY)) {
                final EClass actorType = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                        .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
                checkArgument(AsmUtils.isActorType(actorType), "Owner class must be an access point");

                final Object _principal = inputParameter.get(Dispatcher.PRINCIPAL_KEY);
                if (!(_principal instanceof JudoPrincipal)) {
                    throw new UnsupportedOperationException("Unsupported or unknown actor");
                }

                actor = actorResolver.authenticateByPrincipal((JudoPrincipal) _principal);
            } else {
                actor = Optional.empty();
            }

            if (actor.isPresent()) {
                final EClass principal = Optional.ofNullable(operation.getEType())
                        .filter(t -> t instanceof EClass).map(t -> (EClass) t)
                        .orElseThrow(() -> new IllegalArgumentException("Access point not found"));

                final Optional<Payload> result = dao.getByIdentifier(principal, actor.get().getAs(identifierProvider.getType(), identifierProvider.getName()));
                if (result.isPresent()) {
                    // copy transient attributes (from claims)
                    result.get().putAll(principal.getEAllAttributes().stream()
                            .filter(a -> AsmUtils.annotatedAsTrue(a, "transient") && actor.get().get(a.getName()) != null)
                            .collect(Collectors.toMap(a -> a.getName(), a -> actor.get().get(a.getName()))));
                }

                ret = result.orElse(null);
            } else {
                ret = null;
            }
        }
        return CallInterceptorUtil.postCallInterceptors(asmModel, operation, interceptorProvider, inputParameter, ret);
    }
}
