package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.services.dao.fixture.RdbmsDatasourceSingetonExtension;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static hu.blackbelt.judo.dao.api.Payload.empty;
import static hu.blackbelt.judo.dao.api.Payload.map;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class StatefulFlagTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    protected String getModelName() {
        return MODEL_NAME;
    }

    protected Model getEsmModel() {
        final StringType stringType = newStringTypeBuilder().withName("String").withMaxLength(255).build();

        final Model model = NamespaceBuilders.newModelBuilder().withName(getModelName()).build();

        final EntityType referenced = newEntityTypeBuilder()
                .withName("Referenced")
                .build();
        useEntityType(referenced)
                .withMapping(newMappingBuilder().withTarget(referenced).build())
                .build();

        final EntityType referrer = newEntityTypeBuilder()
                .withName("Referrer")
                .build();
        useEntityType(referrer)
                .withMapping(newMappingBuilder().withTarget(referrer).build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("single")
                        .withLower(0).withUpper(1)
                        .withTarget(referenced)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("many")
                        .withLower(0).withUpper(-1)
                        .withTarget(referenced)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .withMemberType(MemberType.STORED)
                        .build())
                .build();

        model.getElements().addAll(Arrays.asList(
                stringType, referenced, referrer
        ));
        return model;
    }


    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.init(getEsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void testRange(RdbmsDaoFixture daoFixture) {
        final EClass referencedType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Referenced").get();
        final EClass referrerType = daoFixture.getAsmUtils().getClassByFQName(DTO_PACKAGE + ".Referrer").get();
        final EReference singleOfReferrerReference = referrerType.getEAllReferences().stream().filter(r -> "single".equals(r.getName())).findAny().get();
        final EReference manyOfReferrerReference = referrerType.getEAllReferences().stream().filter(r -> "many".equals(r.getName())).findAny().get();

        final UUID referenced1Id = daoFixture.getDao().create(referencedType, empty(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final UUID referenced2Id = daoFixture.getDao().create(referencedType, empty(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        final UUID referrerId = daoFixture.getDao().create(referrerType, empty(), DAO.QueryCustomizer.<UUID>builder()
                .mask(Collections.emptyMap())
                .build()).getAs(daoFixture.getIdProvider().getType(), daoFixture.getIdProvider().getName());
        daoFixture.getDao().setReference(singleOfReferrerReference, referrerId, Collections.singleton(referenced1Id));
        daoFixture.getDao().setReference(manyOfReferrerReference, referrerId, Collections.singleton(referenced2Id));

        daoFixture.getContext().put("STATEFUL", Boolean.FALSE);

        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().create(referencedType, empty(), null), "INSERT is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().update(referencedType, map(daoFixture.getIdProvider().getName(), referenced1Id), null), "UPDATE is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().delete(referencedType, referenced1Id), "DELETE is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().setReference(singleOfReferrerReference, referrerId, Collections.singleton(referenced2Id)), "SET is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().unsetReference(singleOfReferrerReference, referrerId), "UNSET is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().addReferences(manyOfReferrerReference, referrerId, Collections.singleton(referenced1Id)), "ADD is not supported in stateless operation");
        assertThrows(IllegalStateException.class, () -> daoFixture.getDao().removeReferences(manyOfReferrerReference, referrerId, Collections.singleton(referenced1Id)), "REMOVE is not supported in stateless operation");
    }
}
