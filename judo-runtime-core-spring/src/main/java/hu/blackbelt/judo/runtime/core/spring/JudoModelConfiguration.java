package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JudoModelConfiguration {

    @Autowired
    private JudoModelHolder judoModelHolder;

    @Bean
    public AsmModel getAsmModel() {
        return judoModelHolder.getAsmModel();
    }

    @Bean
    public Asm2RdbmsTransformationTrace getAsm2RdbmsTrace() {
        return judoModelHolder.getAsm2rdbms();
    }

    @Bean
    public RdbmsModel getRdbmsModel() {
        return judoModelHolder.getRdbmsModel();
    }

    @Bean
    public ExpressionModel getExpressionModel() {
        return judoModelHolder.getExpressionModel();
    }

    @Bean
    public MeasureModel getMeasureModel() {
        return judoModelHolder.getMeasureModel();
    }

}
