package hu.blackbelt.judo.runtime.core.bootstrap;

import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsSequenceConfig;

import java.util.Optional;

public interface JudoModuleConfig {
    RdbmsSequenceConfig getRdbmsSequenceConfig();
}
