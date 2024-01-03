package hu.blackbelt.judo.runtime.core.dagger2;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;

public interface ModelHolder {

    AsmModel getAsmModel();

    RdbmsModel getRdbmsModel();

    MeasureModel getMeasureModel();

    ExpressionModel getExpressionModel();

    LiquibaseModel getLiquibaseModel();

    Asm2RdbmsTransformationTrace getAsm2rdbms();

}
