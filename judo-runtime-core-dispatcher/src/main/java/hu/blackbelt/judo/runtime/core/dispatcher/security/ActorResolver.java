package hu.blackbelt.judo.runtime.core.dispatcher.security;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.Optional;

/**
 * Actor (instance) resolver.
 */
public interface ActorResolver {

    /**
     * Authenticate actor (check if exists in database, load data).
     *
     * @param exchange exchange
     */
    void authenticateActor(Map<String, Object> exchange);

    /**
     * Authenticate actor by principal (check if exists in database, load data).
     *
     * @param principal JUDO principal
     */
    Optional<Payload> authenticateByPrincipal(JudoPrincipal principal);

    /**
     * Get actor instance by claims (token).
     *
     * @param actorType actor type
     * @param claims    claims
     * @return payload of actor (instance)
     */
    Payload getActorByClaims(EClass actorType, Map<String, Object> claims);
}
