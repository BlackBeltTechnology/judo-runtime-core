package hu.blackbelt.judo.services.persistence.hsqldb;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.services.persistence.LiquibaseModelTrackerWithPrehook;
import lombok.extern.slf4j.Slf4j;
import org.osgi.framework.Bundle;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;


@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Slf4j
public class LiquibaseModelToEmbeddedHsqlDatabaseServiceTracker extends LiquibaseModelTrackerWithPrehook {

    public static final String HSQLDB_SERVER_PID =
            "hu.blackbelt.judo.framework.internal.hsqldb.server.HsqldbServer";
    public static final String HSQLDB = "hsqldb";
    public static final String HSQLDB_PREFIX = HSQLDB + "-";
    public static final String DELEGATED_PROPERTY_KEY_PREFIX = "pax-jdbc.";
    public static final String PAX_JDBC_DATASOURCE = "org.ops4j.datasource";

    int portNumber = 30000;

    @Reference
    ConfigurationAdmin configAdmin;

    Map<String, Object> config;

    Bundle bundle;

    boolean ignoreVersionCheck = true;

    @Activate
    protected void activate(ComponentContext componentContext,  Map<String, Object> config) {
        this.config = config;
        this.bundle = componentContext.getBundleContext().getBundle();
        String ignoreVersionCheckString = (String) config.get(IGNORE_VERSION_CHECK);
        if (ignoreVersionCheckString != null && ignoreVersionCheckString.trim().equals("false")) {
            ignoreVersionCheck = false;
        }

        openTracker(componentContext.getBundleContext());
    }

    @Deactivate
    protected void deactivate() {
        closeTracker();
    }

    @Override
    public void install(LiquibaseModel liquibaseModel) {
        createHsqldbServerConfigurations(liquibaseModel);
        crearePrehookConfiguration(bundle, configAdmin, liquibaseModel, HSQLDB, ignoreVersionCheck);
        portNumber++;
    }

    @Override
    public void uninstall(LiquibaseModel liquibaseModel) {
        removePrehookConfiguration(bundle, configAdmin, liquibaseModel, HSQLDB);
        removeHsqldbServerConfigurations(liquibaseModel);
    }

    @Override
    public Class<LiquibaseModel> getModelClass() {
        return LiquibaseModel.class;
    }



    private void createHsqldbServerConfigurations(LiquibaseModel liquibaseModel) {

        String dataSourceName = HSQLDB_PREFIX + liquibaseModel.getName();
        String databaseName = liquibaseModel.getName() + "-" + liquibaseModel.getVersion();

        final Dictionary<String, Object> dataSourceConfigurationProps = new Hashtable<>();
        dataSourceConfigurationProps.put("dataSourceName", dataSourceName);
        dataSourceConfigurationProps.put("databaseName", databaseName);
        dataSourceConfigurationProps.put("databasePath", "hsqldb" + File.separator + databaseName + File.separator + System.currentTimeMillis() + File.separator + "db");
        dataSourceConfigurationProps.put("port", Integer.valueOf(portNumber));
        dataSourceConfigurationProps.put("pax-jdbc.ops4j.preHook", dataSourceName );
        dataSourceConfigurationProps.put("pax-jdbc.osgi.jdbc.driver.name", "org.hsqldb.hsqldb-native" );
        dataSourceConfigurationProps.put("pax-jdbc.url", "jdbc:hsqldb:hsql://localhost:" + portNumber + "/" + databaseName);
        dataSourceConfigurationProps.put("pax-jdbc.databaseName", databaseName);
        dataSourceConfigurationProps.put("pax-jdbc.user", "sa");
        dataSourceConfigurationProps.put("pax-jdbc.judo.model.name", liquibaseModel.getName());

        config.forEach((k, v) -> {
            if (k.startsWith(DELEGATED_PROPERTY_KEY_PREFIX)) {
                dataSourceConfigurationProps.put(k, v);
            }
        });

        dataSourceConfigurationProps.put(CREATED_BY, this.getClass().getName());

        final Configuration dataSourceConfiguration;
        try {
            dataSourceConfiguration = configAdmin.createFactoryConfiguration(
                    HSQLDB_SERVER_PID, "?");
            dataSourceConfiguration.update(dataSourceConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid datasource name: " + dataSourceName, e);
        }
    }

    private void removeHsqldbServerConfigurations(LiquibaseModel liquibaseModel) {
        String dataSourceName = HSQLDB_PREFIX + liquibaseModel.getName();

        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + HSQLDB_SERVER_PID
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
