package hu.blackbelt.judo.services.persistence;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.tatami.core.AbstractModelTracker;
import lombok.extern.slf4j.Slf4j;
import org.ops4j.pax.jdbc.hook.PreHook;
import org.osgi.framework.Bundle;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

@Slf4j
public abstract class LiquibaseModelTrackerWithPrehook extends AbstractModelTracker<LiquibaseModel> {
    public static final String CREATED_BY = "pax.__createdBy";
    public static final String IGNORE_VERSION_CHECK = "pax.ignoreVersionCheck";

    public void crearePrehookConfiguration(Bundle bundle, ConfigurationAdmin configAdmin, LiquibaseModel liquibaseModel, String dialect, boolean ignoreVersionCheck) {
        final Dictionary<String, Object> prehookConfigurationProps = new Hashtable<>();
        prehookConfigurationProps.put(PreHook.KEY_NAME, dialect + "-" + liquibaseModel.getName());
        prehookConfigurationProps.put("dialect", dialect);
        prehookConfigurationProps.put("ignoreVersionCheck", ignoreVersionCheck);
        prehookConfigurationProps.put("modelStoreInitScriptBundleSymbolicName", bundle.getSymbolicName());
        prehookConfigurationProps.put("rdbmsModel.target", "(name=" + liquibaseModel.getName() + ")");
        prehookConfigurationProps.put("liquibaseModel.target", "(name=" + liquibaseModel.getName() + ")");

        prehookConfigurationProps.put(CREATED_BY, this.getClass().getName());

        final Configuration prehookConfiguration;
        try {
            prehookConfiguration = configAdmin.createFactoryConfiguration(
                    LiquibaseModelDataSourcePrehook.class.getName(), "?");
            prehookConfiguration.update(prehookConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid name: " + liquibaseModel.getName(), e);
        }
    }

    public void removePrehookConfiguration(Bundle bundle, ConfigurationAdmin configAdmin, LiquibaseModel liquibaseModel, String dialect) {
        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + hu.blackbelt.judo.services.persistence.LiquibaseModelDataSourcePrehook.class.getName()
                            + ")("
                            + PreHook.KEY_NAME + "=" + dialect + "-" + liquibaseModel.getName() + ")("
                            + CREATED_BY + "=" + this.getClass().getName() + "))");

            if (cfgsToDelete != null) {
                for (final Configuration c : cfgsToDelete) {
                    c.delete();

                    log.debug("PreHook '{}' removed.", liquibaseModel.getName());
                }
            } else {
                log.warn("No configuration found for datasource: " + liquibaseModel.getName());
            }
        } catch (InvalidSyntaxException | IOException ex) {
            log.error("Invalid syntax: " + liquibaseModel.getName(), ex);
        }
    }
}
