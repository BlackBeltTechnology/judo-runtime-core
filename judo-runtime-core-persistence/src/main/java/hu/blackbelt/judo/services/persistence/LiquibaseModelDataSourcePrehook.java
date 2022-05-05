package hu.blackbelt.judo.services.persistence;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.rdbms.schema.incremental.CheckRdbmsModelCompatibility;
import hu.blackbelt.judo.rdbms.schema.incremental.LiquibaseVersionUpdate;
import hu.blackbelt.judo.rdbms.schema.modelstore.ModelStoreRepository;
import hu.blackbelt.judo.rdbms.schema.modelstore.ModelVersion;
import hu.blackbelt.osgi.liquibase.LiquibaseExecutor;
import liquibase.exception.LiquibaseException;
import lombok.extern.slf4j.Slf4j;
import org.ops4j.pax.jdbc.hook.PreHook;
import org.osgi.framework.Bundle;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Optional;

import static com.diogonunes.jcolor.Ansi.colorize;
import static com.diogonunes.jcolor.Attribute.RED_TEXT;
import static hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModelStreamProvider.getStreamFromLiquibaseModel;
import static hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.*;
import static hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel.LoadArguments.rdbmsLoadArgumentsBuilder;
import static hu.blackbelt.judo.rdbms.schema.incremental.RdbmsModelStreamProvider.getStreamsFromRdbmsModel;
import static hu.blackbelt.judo.rdbms.schema.modelstore.VersionFactory.createVersion;

@Slf4j
@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class LiquibaseModelDataSourcePrehook implements PreHook {
    public static final String CREATED_BY = ".__createdBy";

    public static final String JUDO_MODEL_VERSIONS = "JUDO_MODEL_VERSIONS";
    public static final String RDBMS_MODEL_TYPE = "rdbms";

    @Reference
    private LiquibaseModel liquibaseModel;

    @Reference
    private RdbmsModel rdbmsModel;

    @Reference
    private LiquibaseExecutor liquibaseExecutor;

    @Reference
    ConfigurationAdmin configAdmin;

    private String dialect;

    private boolean ignoreVersionCheck = false;

    Bundle modelStoreInitScriptBundle;

    @Activate
    public void activate(ComponentContext componentContext) {
        dialect = (String) componentContext.getProperties().get("dialect");
        if (dialect == null || dialect.trim().equals("")) {
            throw new IllegalArgumentException("dialect is not defined");
        }

        Object ignoreVersionCheckO = componentContext.getProperties().get("ignoreVersionCheck");
        if (ignoreVersionCheckO instanceof Boolean) {
            ignoreVersionCheck = (Boolean) ignoreVersionCheckO;
        }
        if (ignoreVersionCheckO instanceof String) {
            if (ignoreVersionCheckO == null || ((String) ignoreVersionCheckO).trim().equals("")) {
                ignoreVersionCheck = false;
            } else if (ignoreVersionCheckO.equals("true")) {
                ignoreVersionCheck = true;
            }
        }

        String modelStoreInitScriptBundleSymbolicName = (String) componentContext.getProperties().get("modelStoreInitScriptBundleSymbolicName");
        if (modelStoreInitScriptBundleSymbolicName == null || modelStoreInitScriptBundleSymbolicName.trim().equals("")) {
            throw new IllegalArgumentException("modelStoreInitScriptBundleSymbolicName is not defined");
        }
        modelStoreInitScriptBundle = Arrays.stream(componentContext.getBundleContext()
                .getBundles()).filter(b -> b.getSymbolicName().equals(modelStoreInitScriptBundleSymbolicName))
                .findFirst().orElseThrow(() -> new IllegalStateException("Bundle " + modelStoreInitScriptBundleSymbolicName + " not founf"));
    }

    @Deactivate
    public void deactivate() {
        removeModelStoreVersionSaveTrackerConfiguration();
    }

    @Override
    public void prepare(DataSource dataSource) throws SQLException {

        if (ignoreVersionCheck) {
            LiquibaseVersionUpdate.executeModel(liquibaseModel, dataSource);
            return;
        }

        // Create liquibase model store if required
        try (Connection connection = dataSource.getConnection()) {
            liquibaseExecutor.executeLiquibaseScript(connection,
                    "liquibase/" + dialect + "-model-store-changelog.xml", modelStoreInitScriptBundle,
                    ImmutableMap.of("table-name", JUDO_MODEL_VERSIONS)
            );
        } catch (SQLException | LiquibaseException e) {
            log.error("Could not execute liquibase script", e);
            return;
        }

        ModelStoreRepository modelStoreRepository = new ModelStoreRepository(dataSource, JUDO_MODEL_VERSIONS);

        // Check latest version
        Optional<ModelVersion> modelVersion = modelStoreRepository.getLatestModelVersion(liquibaseModel.getName());
        RdbmsModel rdbmsModelInDatabase = null;

        if (!modelVersion.isPresent()) {
            LiquibaseVersionUpdate.executeModel(liquibaseModel, dataSource);

            modelStoreRepository.addVersion(
                    ModelVersion.buildVersion()
                            .modelName(liquibaseModel.getName())
                            .version(createVersion(liquibaseModel.getVersion()))
                            .addEntry(ModelVersion.ModelEntry.buildEntry()
                                    .modelType("rdbms")
                                    .data(getStreamsFromRdbmsModel(rdbmsModel))
                                    .build())
                            .addEntry(ModelVersion.ModelEntry.buildEntry()
                                    .modelType("liquibase")
                                    .data(getStreamFromLiquibaseModel(liquibaseModel))
                                    .build())
                            .build()
            );
            createModelStoreVersionSaveTrackerConfiguration(liquibaseModel.getVersion());
        } else {
            try {
                rdbmsModelInDatabase = loadRdbmsModel(
                        rdbmsLoadArgumentsBuilder()
                                .name("Original")
                                .inputStream(modelVersion.get().getEntryByModelType(RDBMS_MODEL_TYPE).getData())
                                .build());


                StringBuilder report = new StringBuilder();
                if (!CheckRdbmsModelCompatibility.isRdbmsModelIdentical(rdbmsModelInDatabase, rdbmsModel, dialect, report)) {
                    throw new IllegalStateException("There are RDBMS model changes which have not been applied to database\n\n" + colorize(report.toString(), RED_TEXT()));
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not load RDBMS model", e);
            } catch (RdbmsModel.RdbmsValidationException e) {
                throw new IllegalStateException("Invalid RDBMS model", e);
            }
        }
    }

    public void createModelStoreVersionSaveTrackerConfiguration(String version) {
        final Dictionary<String, Object> modelStoreVersionSaveTrackerConfigurationProps = new Hashtable<>();
        modelStoreVersionSaveTrackerConfigurationProps.put("dataSource.target", "(dataSourceName=" + dialect + "-" + liquibaseModel.getName() + ")");
        modelStoreVersionSaveTrackerConfigurationProps.put("name", liquibaseModel.getName());
        modelStoreVersionSaveTrackerConfigurationProps.put("version", version);
        modelStoreVersionSaveTrackerConfigurationProps.put("table", JUDO_MODEL_VERSIONS);
        modelStoreVersionSaveTrackerConfigurationProps.put(CREATED_BY, this.getClass().getName());

        final Configuration modelStoreVersionSaveTrackerConfiguration;
        try {
            modelStoreVersionSaveTrackerConfiguration = configAdmin.createFactoryConfiguration(
                    ModelStoreVersionSaveTracker.class.getName(), "?");
            modelStoreVersionSaveTrackerConfiguration.update(modelStoreVersionSaveTrackerConfigurationProps);
        } catch (IOException e) {
            log.error("Invalid name: " + liquibaseModel.getName(), e);
        }
    }

    public void removeModelStoreVersionSaveTrackerConfiguration() {
        try {
            final Configuration[] cfgsToDelete = configAdmin.listConfigurations(
                    "(&(service.factoryPid=" + ModelStoreVersionSaveTracker.class.getName()
                            + ")("
                            + "name=" + liquibaseModel.getName() + ")("
                            + CREATED_BY + "=" + this.getClass().getName() + "))");

            if (cfgsToDelete != null) {
                for (final Configuration c : cfgsToDelete) {
                    c.delete();

                    log.debug("ModelStoreVersionSaveTracker '{}' removed.", liquibaseModel.getName());
                }
            } else {
                log.warn("No configuration found for ModelStoreVersionSaveTracker: " + liquibaseModel.getName());
            }
        } catch (InvalidSyntaxException | IOException ex) {
            log.error("Invalid syntax: " + liquibaseModel.getName(), ex);
        }
    }
}
