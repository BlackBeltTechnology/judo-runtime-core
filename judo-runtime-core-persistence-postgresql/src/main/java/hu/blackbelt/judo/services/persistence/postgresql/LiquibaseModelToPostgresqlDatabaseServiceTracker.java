package hu.blackbelt.judo.services.persistence.postgresql;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.services.persistence.LiquibaseModelTrackerWithPrehook;
import lombok.extern.slf4j.Slf4j;
import org.osgi.framework.Bundle;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;


@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Slf4j
public class LiquibaseModelToPostgresqlDatabaseServiceTracker extends LiquibaseModelTrackerWithPrehook {

    public static final String POSTGRESQL = "postgresql";
    public static final String POSTGRESQL_PREFIX = POSTGRESQL + "-";
    public static final String PAX_JDBC_DATASOURCE = "org.ops4j.datasource";
    public static final String DELEGATED_PROPERTY_KEY_PREFIX = "pax-jdbc.";

    int portNumber = 5432;

    @Reference
    ConfigurationAdmin configAdmin;

    Map<String, Object> config;

    Bundle bundle;

    boolean ignoreVersionCheck = false;

    @Activate
    protected void activate(ComponentContext componentContext,  Map<String, Object> config) {
        this.config = config;
        this.bundle = componentContext.getBundleContext().getBundle();
        String ignoreVersionCheckString = (String) config.get(IGNORE_VERSION_CHECK);
        if (ignoreVersionCheckString != null && ignoreVersionCheckString.trim().equals("true")) {
            ignoreVersionCheck = true;
        }
        openTracker(componentContext.getBundleContext());
    }

    @Deactivate
    protected void deactivate() {
        closeTracker();
    }

    @Override
    public void install(LiquibaseModel liquibaseModel) {
        crearePrehookConfiguration(bundle, configAdmin, liquibaseModel, POSTGRESQL, ignoreVersionCheck);
        createPostgresqlConfigurations(liquibaseModel);
    }

    @Override
    public void uninstall(LiquibaseModel liquibaseModel) {
        removePrehookConfiguration(bundle, configAdmin, liquibaseModel, POSTGRESQL);
        removePostgresqlConfigurations(liquibaseModel);
    }

    @Override
    public Class<LiquibaseModel> getModelClass() {
        return LiquibaseModel.class;
    }


    private void createPostgresqlConfigurations(LiquibaseModel liquibaseModel) {


        String dataSourceName = POSTGRESQL_PREFIX + liquibaseModel.getName();
        String databaseName = liquibaseModel.getName() + "-" + liquibaseModel.getVersion();

        // TODO: Handling config parameters
        final Dictionary<String, Object> dataSourceConfigurationProps = new Hashtable<>();
        dataSourceConfigurationProps.put("dataSourceName", dataSourceName);
        dataSourceConfigurationProps.put("ops4j.preHook", dataSourceName );
        dataSourceConfigurationProps.put("osgi.jdbc.driver.name", "PostgreSQL JDBC Driver" );
        dataSourceConfigurationProps.put("judo.model.name", liquibaseModel.getName());

        dataSourceConfigurationProps.put(CREATED_BY, this.getClass().getName());
        portNumber++;

        final Dictionary<String, Object> props = new Hashtable<>();
        config.forEach((k, v) -> {
            if (k.startsWith(DELEGATED_PROPERTY_KEY_PREFIX)) {
                dataSourceConfigurationProps.put(k.substring(DELEGATED_PROPERTY_KEY_PREFIX.length()), v);
            }
        });

        final Configuration dataSourceConfiguration;
        try {
            dataSourceConfiguration = configAdmin.createFactoryConfiguration(
                    PAX_JDBC_DATASOURCE, "?");
            dataSourceConfiguration.update(dataSourceConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid datasource name: " + dataSourceName, e);
        }
    }

    private void removePostgresqlConfigurations(LiquibaseModel liquibaseModel) {
        String dataSourceName = POSTGRESQL_PREFIX + liquibaseModel.getName();

        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + PAX_JDBC_DATASOURCE
                            + ")(dataSourceName="
                            + dataSourceName + ")(" + CREATED_BY + "=" + this.getClass().getName() + "))");

            if (cfgsToDelete != null) {
                for (final Configuration c : cfgsToDelete) {
                    c.delete();

                    log.debug("Datasource '{}' removed.", dataSourceName);
                }
            } else {
                log.warn("No configuration found for datasource: " + dataSourceName);
            }
        } catch (InvalidSyntaxException | IOException ex) {
            log.error("Invalid datasource name: " + dataSourceName, ex);
        }

    }
}
