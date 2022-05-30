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
import java.net.URL;

@SpringBootApplication
public class JudoRuntimeCoreSpringApplication {

	@Bean
	public static JudoModelHolder defaultJudoModelHolder() throws Exception {
		JudoModelHolder modelHolder = JudoModelHolder.
				loadFromURL("SalesModel",
						JudoRuntimeCoreSpringApplication.class.getClassLoader().getResource("model").toURI(),
						new HsqldbDialect());
		return modelHolder;
	}

	public static void main(String[] args) {
		SpringApplication.run(JudoRuntimeCoreSpringApplication.class, args);
	}

}
