package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;

public class RdbmsResolverProvider implements Provider<RdbmsResolver> {

    @Inject
    AsmModel asmModel;

    @Inject
    TransformationTraceService transformationTraceService;

    @Override
    public RdbmsResolver get() {
        return RdbmsResolver.builder()
                .asmModel(asmModel)
                .transformationTraceService(transformationTraceService)
                .build();
    }
}
