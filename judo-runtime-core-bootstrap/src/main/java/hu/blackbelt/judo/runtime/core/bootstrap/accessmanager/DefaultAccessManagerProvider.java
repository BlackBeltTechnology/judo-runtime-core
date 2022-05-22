package hu.blackbelt.judo.runtime.core.bootstrap.accessmanager;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.accessmanager.DefaultAccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;

public class DefaultAccessManagerProvider implements Provider<AccessManager> {

    AsmModel asmModel;

    @Inject
    public DefaultAccessManagerProvider(AsmModel asmModel) {
        this.asmModel = asmModel;
    }


    @Override
    public AccessManager get() {
        return DefaultAccessManager.builder()
                .asmModel(asmModel)
                .build();
    }
}
