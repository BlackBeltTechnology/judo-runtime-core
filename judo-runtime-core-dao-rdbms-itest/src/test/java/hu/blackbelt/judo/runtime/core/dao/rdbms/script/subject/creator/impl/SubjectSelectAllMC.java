package hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.impl;

import hu.blackbelt.judo.meta.psm.PsmTestModelBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.script.subject.creator.SubjectCreator.SubjectCollectionCreator;

public class SubjectSelectAllMC implements SubjectCollectionCreator {
    private final PsmTestModelBuilder modelBuilder;

    public SubjectSelectAllMC() {
        modelBuilder = new PsmTestModelBuilder();
        modelBuilder.addEntity("Entity");
    }

    @Override
    public PsmTestModelBuilder getPsmTestModelBuilder() {
        return modelBuilder;
    }

    @Override
    public String getPrep() {
        return "new demo::entities::Entity()\n" +
                "new demo::entities::Entity()\n";
    }

    @Override
    public String getUse() {
        return "demo::entities::Entity";
    }

    @Override
    public String getName() {
        return "SubjectSelectAllMC";
    }

    @Override
    public String getReturnType() {
        return "demo::entities::Entity[]";
    }

    @Override
    public int getCollectionSize() {
        return 2;
    }
}
