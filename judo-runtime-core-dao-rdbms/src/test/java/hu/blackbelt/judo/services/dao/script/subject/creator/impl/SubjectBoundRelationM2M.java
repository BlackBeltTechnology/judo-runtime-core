package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectBoundRelationM2M implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectBoundRelationM2M() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity1");
        modelBuilder.addEntity("Entity")
                .withRelation("Entity1", "e", cardinality(0, 1));
        modelBuilder.addMappedTransferObject("TransferEntity1", "Entity1");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity")
                .withRelation("TransferEntity1", "te", cardinality(0, 1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity mto = new demo::services::TransferEntity(te = new demo::services::TransferEntity1())\n";
    }

    @Override
    public String getUse() {
        return "mto.te";
    }

    @Override
    public String getName() {
        return "SubjectBoundRelationM2M";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity1";
    }
}
