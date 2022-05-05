package hu.blackbelt.judo.services.security;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.osgi.service.component.annotations.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Slf4j
public class PathInfoRealmExtractor implements RealmExtractor {

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    AsmModel asmModel;

    EList<EClass> actors;

    @Activate
    void start() {
        final AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        actors = asmUtils.getAllActorTypes();
    }

    @Deactivate
    void stop() {
        actors = null;
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
