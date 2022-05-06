package hu.blackbelt.judo.services.security;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

@Slf4j
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
