package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator;

import static hu.blackbelt.judo.meta.psm.PsmTestModelBuilder.Cardinality.cardinality;

public class SubjectUnboundRelationU2U implements SubjectCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectUnboundRelationU2U() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addUnmappedTransferObject("TransferObject1");
        modelBuilder.addUnmappedTransferObject("TransferObject")
                .withRelation("TransferObject1", "u", cardinality(0, 1));
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "var demo::services::TransferObject uto = " +
                "\tnew demo::services::TransferObject(u = new demo::services::TransferObject1())\n";
    }

    @Override
    public String getUse() {
        return "uto.u";
    }

    @Override
    public String getName() {
        return "SubjectUnboundRelationU2U";
    }

    @Override
    public String getReturnType() {
        return "demo::services::TransferObject1";
    }
}
