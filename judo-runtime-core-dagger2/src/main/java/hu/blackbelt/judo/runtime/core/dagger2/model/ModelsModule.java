package hu.blackbelt.judo.runtime.core.dagger2.model;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.ModelHolder;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;

@Module
public class ModelsModule {
    @JudoApplicationScope
    @Provides
    public AsmModel providesAsmModel(ModelHolder modelHolder) {
        return modelHolder.getAsmModel();
    }

    @JudoApplicationScope
    @Provides
    public ExpressionModel providesExpressionModel(ModelHolder modelHolder) {
        return modelHolder.getExpressionModel();
    }

    @JudoApplicationScope
    @Provides
    public RdbmsModel providesRdbmsModel(ModelHolder modelHolder) {
        return modelHolder.getRdbmsModel();
    }

    @JudoApplicationScope
    @Provides
    public MeasureModel providesMeasureModel(ModelHolder modelHolder) {
        return modelHolder.getMeasureModel();
    }

    @JudoApplicationScope
    @Provides
    public LiquibaseModel providesLiquibaseModel(ModelHolder modelHolder) {
        return modelHolder.getLiquibaseModel();
    }

    @JudoApplicationScope
    @Provides
    public Asm2RdbmsTransformationTrace providesAsm2RdbmsTransformationTrace(ModelHolder modelHolder) {
        return modelHolder.getAsm2rdbms();
    }

}
