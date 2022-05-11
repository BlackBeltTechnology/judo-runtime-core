package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationM2MC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationM2MC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");

        modelBuilder.addEntity("Entity1");
        modelBuilder.addMappedTransferObject("TransferEntity1", "Entity1")
                .withRelation("TransferEntity", "te", cardinality(0, -1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity1 mto = new demo::services::TransferEntity1()\n" +
                "mto.te += new demo::services::TransferEntity()\n" +
                "mto.te += new demo::services::TransferEntity()\n";
    }

    @Override
    public String getUse() {
        return "mto.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationM2MC";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity[]";
    }

    @Override
    public int getCollectionSize() {
        return 2;
    }
}
