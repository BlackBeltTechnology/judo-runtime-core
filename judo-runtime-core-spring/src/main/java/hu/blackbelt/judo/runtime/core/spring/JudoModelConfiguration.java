package hu.blackbelt.judo.runtime.core.spring;

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
