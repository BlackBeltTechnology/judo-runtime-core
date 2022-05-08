package hu.blackbelt.judo.runtime.core.bootstrap;

import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsSequenceConfig;
import net.jmob.guice.conf.core.BindConfig;
import net.jmob.guice.conf.core.InjectConfig;

import java.util.Optional;

import static net.jmob.guice.conf.core.Syntax.JSON;

@BindConfig(value = "Judo", syntax = JSON)
public class JudoDefaultModuleConfig implements JudoModuleConfig {

    @InjectConfig("rdbmsSequence")
    private RdbmsSequenceConfig rdbmsSequenceConfig;

    public RdbmsSequenceConfig getRdbmsSequenceConfig() {
        return rdbmsSequenceConfig;
    }
}
