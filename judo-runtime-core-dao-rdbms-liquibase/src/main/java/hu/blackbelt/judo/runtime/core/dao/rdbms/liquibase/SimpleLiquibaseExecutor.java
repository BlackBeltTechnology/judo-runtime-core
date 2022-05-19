package hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.util.Collections;
import java.util.function.Consumer;

import static hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel.SaveArguments.liquibaseSaveArgumentsBuilder;
import static hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseNamespaceFixUriHandler.fixUriOutputStream;

@Slf4j
public class SimpleLiquibaseExecutor {


    public void executeInitiLiquibase(ClassLoader classLoader, String name, DataSource dataSource) {
        try {
            Connection connection = dataSource.getConnection();
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

            @SuppressWarnings("resource")
            final Liquibase liquibase = new Liquibase(name,
                    new ClassLoaderResourceAccessor(classLoader), database);
            liquibase.update(new Contexts(), new LabelExpression());
            database.close();
        } catch (Exception e) {
            log.error("Error init liquibase", e);
            throw new RuntimeException(e);
        }
    }

    public void createDatabase(DataSource dataSource, LiquibaseModel liquibaseModel) {
        executueOnLiquibaseModel(dataSource, liquibaseModel, (l) -> {
            try {
                l.update((String) null);
            } catch (LiquibaseException e) {
                throw new RuntimeException(e);
            }
        });

    }

    public void dropDatabase(DataSource dataSource, LiquibaseModel liquibaseModel) {
        executueOnLiquibaseModel(dataSource, liquibaseModel, (l) -> {
            try {
                l.dropAll();
            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void executueOnLiquibaseModel(DataSource dataSource, LiquibaseModel liquibaseModel, Consumer<Liquibase> consumer) {
        if (!liquibaseModel.getResourceSet().getAllContents().hasNext()) {
            log.warn("Liquibase model is empty");
            return;
        }

        if (!liquibaseModel.isValid()) {
            log.warn("Liquibase model is invalid");
            return;
        }
        try {
            Connection connection = dataSource.getConnection();
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

            ByteArrayOutputStream liquibaseStream = new ByteArrayOutputStream();
            liquibaseModel.saveLiquibaseModel(liquibaseSaveArgumentsBuilder()
                    .validateModel(false)
                    .outputStream(fixUriOutputStream(liquibaseStream)));
            String liquibaseName = liquibaseModel.getName() + ".changelog.xml";

            @SuppressWarnings("resource")
            final Liquibase liquibase = new Liquibase(liquibaseName,
                    new StreamResourceAccessor(Collections.singletonMap(liquibaseName, new ByteArrayInputStream(liquibaseStream.toByteArray()))),
                    database);

            consumer.accept(liquibase);
            database.close();
        } catch (Exception e) {
            log.error("Execute liquibase", e);
            throw new RuntimeException(e);
        }
    }


}
