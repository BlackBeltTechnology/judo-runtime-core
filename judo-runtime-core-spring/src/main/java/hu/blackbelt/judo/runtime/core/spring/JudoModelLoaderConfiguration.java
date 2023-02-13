package hu.blackbelt.judo.runtime.core.spring;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
@Slf4j
public class JudoModelLoaderConfiguration {

    @Autowired
    Dialect dialect;

    @Value("${judo.modelName:}")
    private String modelName;

    @Bean
    public JudoModelLoader defaultJudoModelLoader() throws Exception {

        if (modelName == null || modelName.isBlank()) {
            log.info("Model name not set, searching...");
            Pattern p = Pattern.compile( "(.*)-asm.model");
            for (Resource resource: new PathMatchingResourcePatternResolver(this.getClass().getClassLoader())
                    .getResources("classpath*:/model/*.model")){
                Matcher m = p.matcher(resource.getFilename());
                if (m.matches() ) {
                    if (modelName != null && !modelName.isBlank()) {
                        log.warn("Module already set, ignoring " + m.group(1) + ". To change set judo.modelName parameter " +
                                "in application properties");
                    }
                    log.info("Model name found: " + modelName);
                    modelName = m.group(1);
                }
            }
        }
        if (modelName == null || modelName.isBlank()) {
            throw new IllegalArgumentException("modelName is not set and could not be determined");
        }

        JudoModelLoader modelHolder = JudoModelLoader.
                loadFromClassloader(modelName,
                        JudoModelLoader.class.getClassLoader(),
                        dialect, true);
        return modelHolder;
    }
}
