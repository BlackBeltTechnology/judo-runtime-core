package hu.blackbelt.judo.runtime.core.security;

import org.eclipse.emf.ecore.EClass;

import java.util.Map;

public interface OpenIdConfigurationProvider {

    String getOpenIdConfigurationUrl(EClass actorType);

    Map<String, Object> getOpenIdConfiguration(EClass actorType);

    String getServerUrl();

    String getClientId(EClass actorType);

    void ping();
}
