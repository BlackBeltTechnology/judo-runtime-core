package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.io.File;

@SpringBootApplication
//@Import({ JudoLocalModelLoaderConfiguration.class, JudoDefaultSpringConfiguration.class })
public class JudoRuntimeCoreSpringApplication {

	/*
	@Bean
	public DataSource getDataSource() {
		DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
		dataSourceBuilder.username("SA");
		dataSourceBuilder.password("");
		return dataSourceBuilder.build();
	}

	 */

	@Bean
	public static JudoModelHolder defaultJudoModelHolder() throws Exception {
		JudoModelHolder modelHolder = JudoModelHolder.
				loadFromURL("SalesModel", new File("target/model").toURI(), new HsqldbDialect());
		return modelHolder;
	}

	public static void main(String[] args) {
		SpringApplication.run(JudoRuntimeCoreSpringApplication.class, args);
	}

}
