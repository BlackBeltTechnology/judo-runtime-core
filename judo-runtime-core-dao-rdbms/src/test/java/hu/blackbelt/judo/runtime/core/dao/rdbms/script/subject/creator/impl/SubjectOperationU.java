package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectOperationU implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectOperationU() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
        modelBuilder.addUnboundOperation("action")
                .withBody("return new demo::services::TransferObject()\n")
                .withOutput("TransferObject", cardinality(0, 1));
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
        return "demo::services::UnboundServices.action()";
    }

    @Override
    public String getName() {
        return "SubjectOperationU";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject";
    }
}
