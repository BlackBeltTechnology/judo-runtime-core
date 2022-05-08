package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

public interface RdbmsSequenceConfig {

    Integer getStart();

    Integer getIncrement();

    Boolean getCreateIfNotExists();


}
