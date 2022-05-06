package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

public class SubjectVariableU implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectVariableU() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject uto = new demo::services::TransferObject()\n";
    }

    @Override
    public String getUse() {
        return "uto";
    }

    @Override
    public String getName() {
        return "SubjectVariableU";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject";
    }
}
