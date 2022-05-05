package hu.blackbelt.judo.services.persistence.postgresql;

import com.google.common.collect.Maps;
import hu.blackbelt.judo.services.persistence.PersistenceReady;
import hu.blackbelt.judo.services.transaction.ManagedDatasourceFactory;
import hu.blackbelt.osgi.liquibase.LiquibaseExecutor;
import hu.blackbelt.osgi.utils.osgi.api.PropertiesUtil;
import liquibase.exception.LiquibaseException;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamCastMode;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.ops4j.pax.transx.jdbc.ManagedDataSourceBuilder;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.util.tracker.ServiceTracker;

import javax.resource.spi.TransactionSupport;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;


@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = ModelDataSourcePostgresqlInitTracker.Config.class)
@Slf4j
public class ModelDataSourcePostgresqlInitTracker {

    @ObjectClassDefinition
    public @interface Config {

        @AttributeDefinition(name = "jOOQ enabled", description = "jOOQ enabled for persistence", type = AttributeType.BOOLEAN)
        boolean jooq_enabled() default false;
    }

    public static final String CREATED_BY = ".__createdBy";
    public static final String JUDO_MODEL_NAME = "judo.model.name";
    public static final String DAO_DATASOURCE_NAME = "dao.datasource.name";
    public static final String DAO_DATASOURCE_DIALECT = "dao.datasource.dialect";
    public static final String DAO_DATASOURCE_JOOQ_ENABLED = "dao.datasource.jooq";
    public static final String MANAGED_DATASOURCE_NAME = "managed.datasource.name";
    public static final String MANAGED_DATASOURCE_DIALECT = "managed.datasource.dialect";

    private ComponentContext componentContext;
    private ModelDataSourceTracker modelDataSourceTracker;

    private final Map<String, ServiceRegistration<DataSource>> jooqDataSourceByModelName = Maps.newConcurrentMap();
    private final Map<String, ServiceRegistration<DataSource>> managedDataSourceByModelName = Maps.newConcurrentMap();

    @Reference
    LiquibaseExecutor liquibaseExecutor;

    @Reference
    ManagedDatasourceFactory managedDatasourceFactory;

    private ServiceRegistration<PersistenceReady> ready;

    private boolean jooqEnabled;

    @Activate
    protected void activate(ComponentContext contextPar, Config config) {
        componentContext = contextPar;
        jooqEnabled = config.jooq_enabled();
        modelDataSourceTracker = new ModelDataSourceTracker(contextPar.getBundleContext());
        modelDataSourceTracker.open(true);
    }

    @Deactivate
    protected void deactivate() {
        modelDataSourceTracker.close();
    }

    public void install(String modelName, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            liquibaseExecutor.executeLiquibaseScript(connection, "liquibase/postgresql-init-changelog.xml", componentContext.getBundleContext().getBundle());
            addJooqDatasource(modelName, dataSource);

            Dictionary<String, Object> props = new Hashtable<>();
            props.put(JUDO_MODEL_NAME, modelName);
            ready = componentContext.getBundleContext().registerService(PersistenceReady.class, new PersistenceReady() {}, props);
        } catch (SQLException | LiquibaseException e) {
            log.error("Could not execute liquibase script", e);
        }
    }

    public void uninstall(String modelName, DataSource dataSource) {
        if (ready != null) {
            try {
                ready.unregister();
            } catch (IllegalStateException e) {
                if (!e.getMessage().startsWith("Service already unregistered")) {
                    throw e;
                }
            }
            ready = null;
        }

        removeJooqDatasource(modelName);
    }


    private SQLDialect getJooqSqlDialect() {
        return SQLDialect.POSTGRES;
    }

    private void addJooqDatasource(String modelName, DataSource dataSource) {
        DSLContext jooqContext;

        DataSource managedDs = managedDatasourceFactory.create(dataSource, modelName + "-managed");

        DataSource jooqDataSource;
        if (jooqEnabled) {
            final Settings settings = new Settings();
            settings.setParamCastMode(ParamCastMode.NEVER);
            jooqContext = DSL.using(managedDs, getJooqSqlDialect(), settings);
            jooqDataSource = jooqContext.parsingDataSource();
        } else {
            jooqDataSource = managedDs;
        }

        final Dictionary<String, Object> jooqDatasourceConfiguration = new Hashtable<>();
        jooqDatasourceConfiguration.put(CREATED_BY, this.getClass().getName());
        jooqDatasourceConfiguration.put(DAO_DATASOURCE_NAME, modelName);
        jooqDatasourceConfiguration.put(DAO_DATASOURCE_DIALECT, "postgresql");
        jooqDatasourceConfiguration.put(DAO_DATASOURCE_JOOQ_ENABLED, String.valueOf(jooqEnabled));

        final Dictionary<String, Object> managedDatasourceConfiguration = new Hashtable<>();
        managedDatasourceConfiguration.put(CREATED_BY, this.getClass().getName());
        managedDatasourceConfiguration.put(MANAGED_DATASOURCE_NAME, modelName);
        managedDatasourceConfiguration.put(MANAGED_DATASOURCE_DIALECT, "postgresql");

        if (jooqDataSourceByModelName.containsKey(modelName)) {
            throw new IllegalArgumentException("Model already registered: " + modelName);
        }
        jooqDataSourceByModelName.put(modelName,
                componentContext.getBundleContext()
                        .registerService(DataSource.class, jooqDataSource, jooqDatasourceConfiguration));

        managedDataSourceByModelName.put(modelName,
                componentContext.getBundleContext()
                        .registerService(DataSource.class, managedDs, managedDatasourceConfiguration));

    }

    private void removeJooqDatasource(String modelName) {
        if (!jooqDataSourceByModelName.containsKey(modelName)) {
            throw new IllegalArgumentException("Jooq Datasource was not registered: " + modelName);
        }
        ServiceRegistration<DataSource> registration = jooqDataSourceByModelName.get(modelName);
        jooqDataSourceByModelName.remove(modelName);
        try {
            registration.unregister();
        } catch (IllegalStateException e) {
            if (!e.getMessage().startsWith("Service already unregistered")) {
                throw e;
            }
        }
        ready = null;
        registration = managedDataSourceByModelName.get(modelName);
        managedDataSourceByModelName.remove(modelName);
        try {
            registration.unregister();
        } catch (IllegalStateException e) {
            if (!e.getMessage().startsWith("Service already unregistered")) {
                throw e;
            }
        }
    }


    public class ModelDataSourceTracker extends ServiceTracker<DataSource, DataSource> {
        public ModelDataSourceTracker(BundleContext context) {
            super(context, DataSource.class, null);
        }

        @Override
        public DataSource addingService(ServiceReference<DataSource> serviceReference) {
            DataSource instance = super.addingService(serviceReference);
            if (serviceReference.getProperty(JUDO_MODEL_NAME) != null) {
                String modelName = serviceReference.getProperty(JUDO_MODEL_NAME).toString();
                install(modelName, instance);
            }
            return instance;
        }

        @Override
        public void removedService(ServiceReference<DataSource> serviceReference, DataSource service) {
            if (serviceReference.getProperty(JUDO_MODEL_NAME) != null) {
                String modelName = serviceReference.getProperty(JUDO_MODEL_NAME).toString();
                uninstall(modelName, service);
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
