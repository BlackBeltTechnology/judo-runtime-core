package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationM2UC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationM2UC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject");

        modelBuilder.addEntity("Entity1");
        modelBuilder.addMappedTransferObject("TransferEntity1", "Entity1")
                .withRelation("TransferObject", "te", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity1 mto = new demo::services::TransferEntity1()\n" +
                "mto.te += new demo::services::TransferObject()\n" +
                "mto.te += new demo::services::TransferObject()\n";
    }

    @Override
    public String getUse() {
        return "mto.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationM2UC";
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
