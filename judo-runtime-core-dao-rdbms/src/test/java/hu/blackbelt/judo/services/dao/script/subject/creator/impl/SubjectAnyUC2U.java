package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

public class SubjectAnyUC2U implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectAnyUC2U() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject[] utos = new demo::services::TransferObject[] " +
                "{ new demo::services::TransferObject(), new demo::services::TransferObject() }";
    }

    @Override
    public String getUse() {
        return "utos!any()";
    }

    @Override
    public String getName() {
        return "SubjectAnyUC2U";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject";
    }
}
