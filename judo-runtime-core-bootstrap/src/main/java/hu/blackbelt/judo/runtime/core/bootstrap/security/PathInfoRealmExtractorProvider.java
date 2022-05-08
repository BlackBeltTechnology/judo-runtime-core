package hu.blackbelt.judo.runtime.core.bootstrap.security;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.security.PathInfoRealmExtractor;
import hu.blackbelt.judo.runtime.core.security.RealmExtractor;

public class PathInfoRealmExtractorProvider implements Provider<RealmExtractor> {

    AsmModel asmModel;

    @Inject
    public PathInfoRealmExtractorProvider(AsmModel asmModel) {
       this. asmModel = asmModel;
    }

    @Override
    public RealmExtractor get() {
        return new PathInfoRealmExtractor(asmModel);
    }
}
