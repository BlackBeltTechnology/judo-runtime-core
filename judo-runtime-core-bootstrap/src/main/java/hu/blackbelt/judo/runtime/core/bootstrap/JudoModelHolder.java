package hu.blackbelt.judo.runtime.core.bootstrap;


import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.script.runtime.ScriptModel;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder
@Getter
public class JudoModelHolder {

    @NonNull
    AsmModel asmModel;

    @NonNull
    RdbmsModel rdbmsModel;

    @NonNull
    MeasureModel measureModel;

    @NonNull
    ExpressionModel expressionModel;

    @NonNull
    ScriptModel scriptModel;

    @NonNull
    Asm2RdbmsTransformationTrace asm2rdbms;

}
