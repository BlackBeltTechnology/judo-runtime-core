package hu.blackbelt.judo.services.dao.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.services.dao.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationM2U implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationM2U() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
        modelBuilder.addUnmappedTransferObject("TransferObject1");
        modelBuilder.addMappedTransferObject("TransferEntity", "Entity")
                .withRelation("TransferObject1", "u", cardinality(0, 1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferEntity mto = " +
                "\tnew demo::services::TransferEntity(u = new demo::services::TransferObject1())\n";
    }

    @Override
    public String getUse() {
        return "mto.u";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationM2U";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject1";
    }
}
