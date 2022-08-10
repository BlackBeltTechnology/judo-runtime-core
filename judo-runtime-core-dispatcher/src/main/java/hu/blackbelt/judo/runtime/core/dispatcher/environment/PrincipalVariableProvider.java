package hu.blackbelt.judo.runtime.core.dispatcher.environment;

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

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;

import java.security.Principal;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PrincipalVariableProvider implements Function<String, Object> {

    @NonNull
    @Setter
    Context context;

    @NonNull
    @Setter
    AsmModel asmModel;

    @NonNull
    @Setter
    Dispatcher dispatcher;

    private AsmUtils asmUtils;

    private AsmUtils getAsmUtils() {
        if (asmUtils == null) {
            asmUtils = new AsmUtils(asmModel.getResourceSet());
        }
        return asmUtils;
    }

    @SuppressWarnings("unchecked")
	@Override
    public Object apply(final String key) {
        final Principal principal = context.getAs(Principal.class, Dispatcher.PRINCIPAL_KEY);

        if (principal instanceof JudoPrincipal) {
            final EClass actorType = getAsmUtils().resolve(((JudoPrincipal) principal).getClient())
                    .filter(c -> c instanceof EClass)
                    .map(c -> (EClass) c)
                    .orElse(null);

            checkState(actorType != null, "Unknown actor: " + ((JudoPrincipal) principal).getClient());

            return actorType.getEOperations().stream()
                    .filter(o -> AsmUtils.getBehaviour(o).filter(AsmUtils.OperationBehaviour.GET_PRINCIPAL::equals).isPresent())
                    .findAny()
                    .map(eOperation -> {
                        final Map<String, Object> map = dispatcher.callOperation(AsmUtils.getOperationFQName(eOperation), Payload.map(Dispatcher.PRINCIPAL_KEY, principal));
                        final String outputParameterName = AsmUtils.getOutputParameterName(eOperation).get();
                        return Payload.asPayload((Map<String, Object>) map.get(outputParameterName));
                    })
                    .map(p -> p.get(key))
                    .orElse(null);
        } else {
            log.warn("Information about current user not not available");
            return null;
        }
    }
}
