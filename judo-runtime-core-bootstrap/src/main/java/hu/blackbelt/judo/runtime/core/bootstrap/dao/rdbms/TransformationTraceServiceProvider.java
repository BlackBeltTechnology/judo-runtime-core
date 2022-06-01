package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.judo.tatami.core.TransformationTraceServiceImpl;

public class TransformationTraceServiceProvider implements Provider<TransformationTraceService> {

    @Inject
    JudoModelLoader models;

    @Override
    public TransformationTraceService get() {
        TransformationTraceService transformationTraceService = new TransformationTraceServiceImpl();
        transformationTraceService.add(models.getAsm2rdbms());
        return transformationTraceService;
    }
}
