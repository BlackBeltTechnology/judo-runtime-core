package hu.blackbelt.judo.services.persistence;

import hu.blackbelt.mapper.api.CoercerFactory;
import lombok.extern.slf4j.Slf4j;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.util.tracker.ServiceTracker;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = DaoDataSourceToRdbmsDAOTracker.Config.class)
@Slf4j
public class DaoDataSourceToRdbmsDAOTracker {

    @ObjectClassDefinition
    public @interface Config {

        @AttributeDefinition(name = "Enabled user manager", type = AttributeType.BOOLEAN)
        boolean userManagerEnabled() default true;

        @AttributeDefinition(name = "Enabled optimistic lock", type = AttributeType.BOOLEAN)
        boolean optimisticLockEnabled() default true;

        @AttributeDefinition(name = "markSelectedRangeItems", description = "Mark already selected (persisted) items by range operation call")
        boolean markSelectedRangeItems() default false;
    }

    public static final String CREATED_BY = ".__createdBy";
    public static final String NAME = "name";
    public static final String JUDO_MODEL_NAME = "judo.model.name";
    public static final String DAO_DATASOURCE_NAME = "dao.datasource.name";
    public static final String DAO_DATASOURCE_DIALECT = "dao.datasource.dialect";
    public static final String DAO_DATASOURCE_JOOQ_ENABLED = "dao.datasource.jooq";

    public static final String DAO_IMPL_PID = "hu.blackbelt.judo.services.dao.rdbms.RdbmsDAOImpl";
    public static final String COMPOSITE_DAO_IMPL_PID = "hu.blackbelt.judo.services.dao.compose.CompositeDao";
    public static final String SEQUENCE_IMPL_PID = "hu.blackbelt.judo.services.dao.rdbms.sequence.RdbmsSequence";

    private ComponentContext componentContext;
    private DaoDataSourceTracker daoDataSourceTracker;

    @Reference
    ConfigurationAdmin configAdmin;

    @Reference
    CoercerFactory coercerFactory;

    private boolean userManagerEnabled;
    private boolean optimisticLockEnabled;
    private boolean markSelectedRangeItems;

    @Activate
    protected void activate(ComponentContext contextPar, Config config) {
        userManagerEnabled = config.userManagerEnabled();
        optimisticLockEnabled = config.optimisticLockEnabled();
        markSelectedRangeItems = config.markSelectedRangeItems();
        componentContext = contextPar;
        daoDataSourceTracker = new DaoDataSourceTracker(contextPar.getBundleContext());
        daoDataSourceTracker.open(true);
    }

    @Deactivate
    protected void deactivate() {
        daoDataSourceTracker.close();
    }

    public void install(String modelName, String sqlDialect, boolean jooqEnabled, DataSource dataSource) {
        addRdbmsDAOConfiguration(modelName, sqlDialect, jooqEnabled, dataSource);
        addCompositeDAOConfiguration(modelName);
    }

    public void uninstall(String modelName) {
        removeRdbmsDAOConfigurations(modelName);
        removeCompositeDAOConfigurations(modelName);
    }

    private void addRdbmsDAOConfiguration(String modelName, String sqlDialect, boolean jooqEnabled, DataSource dataSource) {
        final Dictionary<String, Object> daoConfigurationProps = new Hashtable<>();

        daoConfigurationProps.put(CREATED_BY, this.getClass().getName());
        daoConfigurationProps.put(JUDO_MODEL_NAME, modelName);

        daoConfigurationProps.put("dataTypeManager.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("variableResolver.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("metricsCollector.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("context.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("asmModel.target", "(" + NAME + "=" + modelName + ")");
        daoConfigurationProps.put("rdbmsModel.target", "(" + NAME + "=" + modelName + ")");
        daoConfigurationProps.put("measureModel.target", "(" + NAME + "=" + modelName + ")");
        daoConfigurationProps.put("dataSource.target", "(" + DAO_DATASOURCE_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("sqlDialect", sqlDialect);
        daoConfigurationProps.put("jooq.enabled", jooqEnabled);
        daoConfigurationProps.put("optimisticLockEnabled", optimisticLockEnabled);
        daoConfigurationProps.put("markSelectedRangeItems", markSelectedRangeItems);
        daoConfigurationProps.put("type", "rdbms");

        final Configuration daoConfiguration;
        try {
            daoConfiguration = configAdmin.createFactoryConfiguration(
                    DAO_IMPL_PID, "?");
            daoConfiguration.update(daoConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid datasource name: " + modelName, e);
        }
    }

    private void addCompositeDAOConfiguration(String modelName) {
        final Dictionary<String, Object> daoConfigurationProps = new Hashtable<>();

        daoConfigurationProps.put(CREATED_BY, this.getClass().getName());
        daoConfigurationProps.put(JUDO_MODEL_NAME, modelName);

        daoConfigurationProps.put("dao.target", "(&(" + JUDO_MODEL_NAME + "=" + modelName + ")(type=rdbms))");
        daoConfigurationProps.put("userManager.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("userManagerEnabled", userManagerEnabled);
        daoConfigurationProps.put("context.target", "(" + JUDO_MODEL_NAME + "=" + modelName + ")");
        daoConfigurationProps.put("type", "composite");

        final Configuration daoConfiguration;
        try {
            daoConfiguration = configAdmin.createFactoryConfiguration(
                    COMPOSITE_DAO_IMPL_PID, "?");
            daoConfiguration.update(daoConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid ASM name: " + modelName, e);
        }
    }

    private void removeRdbmsDAOConfigurations(String modelName) {
        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + DAO_IMPL_PID
                            + ")(" + JUDO_MODEL_NAME + "="
                            + modelName + ")(" + CREATED_BY + "=" + this.getClass().getName() + "))");

            if (cfgsToDelete != null) {
                for (final Configuration c : cfgsToDelete) {
                    c.delete();

                    log.debug("Datasource '{}' removed.", modelName);
                }
            } else {
                log.warn("No configuration found for datasource: " + modelName);
            }
        } catch (InvalidSyntaxException | IOException ex) {
            log.error("Invalid datasource name: " + modelName, ex);
        }
    }

    private void removeCompositeDAOConfigurations(String modelName) {
        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + COMPOSITE_DAO_IMPL_PID
                            + ")(" + JUDO_MODEL_NAME + "="
                            + modelName + ")(" + CREATED_BY + "=" + this.getClass().getName() + "))");

            if (cfgsToDelete != null) {
                for (final Configuration c : cfgsToDelete) {
                    c.delete();

                    log.debug("Composite DAO '{}' removed.", modelName);
                }
            } else {
                log.warn("No configuration found for composite DAO: " + modelName);
            }
        } catch (InvalidSyntaxException | IOException ex) {
            log.error("Invalid ASM name: " + modelName, ex);
        }
    }

    public class DaoDataSourceTracker extends ServiceTracker<DataSource, DataSource> {
        public DaoDataSourceTracker(BundleContext context) {
            super(context, DataSource.class, null);
        }

        @Override
        public DataSource addingService(ServiceReference<DataSource> serviceReference) {
            DataSource instance = super.addingService(serviceReference);
            if (serviceReference.getProperty(DAO_DATASOURCE_NAME) != null) {
                String modelName = serviceReference.getProperty(DAO_DATASOURCE_NAME).toString();
                String sqlDialect = serviceReference.getProperty(DAO_DATASOURCE_DIALECT).toString();
                Boolean jooqEnabled = Boolean.parseBoolean(serviceReference.getProperty(DAO_DATASOURCE_JOOQ_ENABLED).toString());
                install(modelName, sqlDialect, jooqEnabled, instance);
            }
            return instance;
        }

        @Override
        public void removedService(ServiceReference<DataSource> serviceReference, DataSource service) {
            if (serviceReference.getProperty(DAO_DATASOURCE_NAME) != null) {
                String modelName = serviceReference.getProperty(DAO_DATASOURCE_NAME).toString();
                uninstall(modelName);
            }
            super.removedService(serviceReference, service);
        }

        @Override
        public void modifiedService(ServiceReference<DataSource> serviceReference,
                                    DataSource service) {
            super.modifiedService(serviceReference, service);
        }

    }
}
