package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectNewU implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectNewU() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "";
    }

    @Override
    public String getUse() {
        return "new demo::services::TransferObject()";
    }

    @Override
    public String getName() {
        return "SubjectNewU";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject";
    }
}
