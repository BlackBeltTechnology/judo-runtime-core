package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationU2M implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationU2M() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity");
        modelBuilder.addUnmappedTransferObject("TransferObject1")
                .withRelation("TransferEntity", "te", cardinality(0, 1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject1 uto1 = new demo::services::TransferObject1(te = new demo::services::TransferEntity())\n";
    }

    @Override
    public String getUse() {
        return "uto1.te";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationU2M";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferEntity";
    }
}
