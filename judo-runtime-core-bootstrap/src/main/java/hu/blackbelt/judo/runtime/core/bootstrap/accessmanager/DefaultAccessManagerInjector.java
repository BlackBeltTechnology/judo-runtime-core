package hu.blackbelt.judo.runtime.core.bootstrap.accessmanager;

import com.google.inject.Inject;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.accessmanager.DefaultAccessManager;

public class DefaultAccessManagerInjector extends DefaultAccessManager {

    @Inject
    public DefaultAccessManagerInjector(AsmModel asmModel) {
        super(asmModel);
    }
}
