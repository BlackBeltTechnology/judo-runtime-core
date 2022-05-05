package hu.blackbelt.judo.services.security;

import org.eclipse.emf.ecore.EClass;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

/**
 * Realm extractor.
 */
public interface RealmExtractor {

    /**
     * Extract actor type name from HTTP request.
     *
     * @param request HTTP request
     * @return actor
     */
    Optional<EClass> extractActorType(HttpServletRequest request);
}
