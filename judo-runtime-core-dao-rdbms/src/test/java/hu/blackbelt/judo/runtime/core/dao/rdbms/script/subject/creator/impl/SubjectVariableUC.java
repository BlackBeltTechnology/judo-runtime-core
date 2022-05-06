package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

public class SubjectVariableUC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectVariableUC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject[] utos = new demo::services::TransferObject[] { \n" +
                "\tnew demo::services::TransferObject(),\n" +
                "\tnew demo::services::TransferObject()\n" +
                "}\n";
    }

    @Override
    public String getUse() {
        return "utos";
    }

    @Override
    public String getName() {
        return "SubjectVariableUC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject[]";
    }

    @Override
    public int getCollectionSize() {
        return 2;
    }
}
