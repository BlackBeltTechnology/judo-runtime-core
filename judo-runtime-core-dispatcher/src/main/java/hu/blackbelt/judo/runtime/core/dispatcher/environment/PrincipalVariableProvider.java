package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;

import java.security.Principal;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class PrincipalVariableProvider implements Function<String, Object> {

    Context context;

    AsmModel asmModel;

    Dispatcher dispatcher;

    private AsmUtils asmUtils;

    private AsmUtils getAsmUtils() {
        if (asmUtils == null) {
            asmUtils = new AsmUtils(asmModel.getResourceSet());
        }
        return asmUtils;
    }

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
