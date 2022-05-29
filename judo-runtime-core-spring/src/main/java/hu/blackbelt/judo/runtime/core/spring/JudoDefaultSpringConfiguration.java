package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.runtime.core.bootstrap.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.guice.annotation.EnableGuiceModules;

@Configuration
@EnableGuiceModules
public class JudoDefaultSpringConfiguration {

    @Bean
    public static JudoDefaultModule judoDefaultModule(JudoModelHolder judoModelHolder) {
        return  JudoDefaultModule.builder()
                .injectModulesTo(null)
                .bindModelHolder(false)
                .judoModelHolder(judoModelHolder)
                .build();
    }

}
