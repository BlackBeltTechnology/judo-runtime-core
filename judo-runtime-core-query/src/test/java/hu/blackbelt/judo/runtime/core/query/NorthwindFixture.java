package hu.blackbelt.judo.runtime.core.query;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.tatami.asm2expression.Asm2ExpressionWork;
import hu.blackbelt.judo.tatami.core.workflow.work.TransformationContext;
import hu.blackbelt.model.northwind.Demo;

import static hu.blackbelt.judo.tatami.psm2asm.etl.Psm2Asm.Psm2AsmParameter.psm2AsmParameter;
import static hu.blackbelt.judo.tatami.psm2asm.etl.Psm2Asm.executePsm2AsmTransformation;
import static hu.blackbelt.judo.tatami.psm2measure.etl.Psm2Measure.Psm2MeasureParameter.psm2MeasureParameter;
import static hu.blackbelt.judo.tatami.psm2measure.etl.Psm2Measure.executePsm2MeasureTransformation;

/**
 * Northwind ASM, measure and expression models (default asm2expression configuration), built once per JVM.
 * Models are shared by read-only tests; a test that mutates a model builds its own with {@link #create()}.
 */
public final class NorthwindFixture {

    public final PsmModel psmModel;
    public final AsmModel asmModel;
    public final MeasureModel measureModel;
    public final ExpressionModel expressionModel;
    public final TransformationContext context;
    public final AsmUtils asmUtils;

    private NorthwindFixture() throws Exception {
        psmModel = new Demo().fullDemo();

        asmModel = AsmModel.buildAsmModel()
                .build();
        measureModel = MeasureModel.buildMeasureModel()
                .name(psmModel.getName())
                .build();

        executePsm2AsmTransformation(psm2AsmParameter()
                .psmModel(psmModel)
                .asmModel(asmModel)
                .parallel(true)
                .useCache(true));
        executePsm2MeasureTransformation(psm2MeasureParameter()
                .psmModel(psmModel)
                .measureModel(measureModel)
                .parallel(true)
                .useCache(true));

        context = new TransformationContext(psmModel.getName());
        context.put(asmModel);
        context.put(measureModel);
        new Asm2ExpressionWork(context).execute();
        expressionModel = context.getByClass(ExpressionModel.class).orElseThrow(IllegalStateException::new);
        asmUtils = new AsmUtils(asmModel.getResourceSet());
    }

    public static NorthwindFixture create() {
        try {
            return new NorthwindFixture();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static NorthwindFixture shared;

    public static synchronized NorthwindFixture shared() {
        if (shared == null) {
            shared = create();
        }
        return shared;
    }
}
