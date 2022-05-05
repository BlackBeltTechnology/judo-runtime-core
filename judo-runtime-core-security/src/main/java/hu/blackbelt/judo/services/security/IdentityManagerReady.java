package hu.blackbelt.judo.services.security;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, service = IdentityManagerReady.class)
public class IdentityManagerReady {
}
