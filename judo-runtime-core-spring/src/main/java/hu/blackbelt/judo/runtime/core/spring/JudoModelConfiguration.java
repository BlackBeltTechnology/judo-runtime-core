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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JudoModelConfiguration {

    @Autowired
    private JudoModelLoader judoModelLoader;

    @Bean
    public AsmModel getAsmModel() {
        return judoModelLoader.getAsmModel();
    }

    @Bean
    public Asm2RdbmsTransformationTrace getAsm2RdbmsTrace() {
        return judoModelLoader.getAsm2rdbms();
    }

    @Bean
    public RdbmsModel getRdbmsModel() {
        return judoModelLoader.getRdbmsModel();
    }

    @Bean
    public ExpressionModel getExpressionModel() {
        return judoModelLoader.getExpressionModel();
    }

    @Bean
    public MeasureModel getMeasureModel() {
        return judoModelLoader.getMeasureModel();
    }

}
