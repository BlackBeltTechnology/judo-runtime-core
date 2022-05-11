package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;

public interface SubjectCreator {

    PsmTestModelBuilder getPsmTestModelBuilder();

    String getPrep();

    String getUse();

    String getName();

    String getReturnType();

    interface SubjectCollectionCreator extends SubjectCreator {
        int getCollectionSize();
    }

}
