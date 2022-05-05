package hu.blackbelt.judo.services.dao;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.namespace.Package;
import hu.blackbelt.judo.meta.esm.structure.DataMember;
import hu.blackbelt.judo.meta.esm.structure.EntityType;
import hu.blackbelt.judo.meta.esm.structure.OneWayRelationMember;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.structure.TwoWayRelationMember;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newPackageBuilder;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.MAPPED;
import static hu.blackbelt.judo.meta.esm.structure.MemberType.STORED;
import static hu.blackbelt.judo.meta.esm.structure.RangeType.DERIVED;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.AGGREGATION;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.ASSOCIATION;
import static hu.blackbelt.judo.meta.esm.structure.RelationKind.COMPOSITION;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newDataMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newEntityTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newMappingBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newOneWayRelationMemberBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTransferObjectTypeBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.newTwoWayRelationMemberBuilder;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.newStringTypeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class TwoWayAssociationAlongWithCompositionTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes.entities";
    public static final String ADMIN_PACKAGE = MODEL_NAME + ".admin";

    private Class<UUID> idProviderClass;
    private String idProviderName;

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        this.idProviderClass = daoFixture.getIdProvider().getType();
        this.idProviderName = daoFixture.getIdProvider().getName();
    }

    @AfterEach
    public void dropDatabase(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    @Test
    public void test(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        // packages
        Package measures = newPackageBuilder().withName("measures").build();
        Package admin = newPackageBuilder().withName("admin").build();
        Package entities = newPackageBuilder().withName("entities").build();

        // integer data type
        StringType string = newStringTypeBuilder().withName("string").withMaxLength(255).build();
        measures.getElements().add(string);

        // account number
        DataMember accountNumber = newDataMemberBuilder()
                .withName("accountNumber")
                .withRequired(true)
                .withIdentifier(true)
                .withDataType(string)
                .withMemberType(STORED)
                .build();

        // bank account entity
        EntityType bankAccount = newEntityTypeBuilder()
                .withName("BankAccount")
                .withAttributes(accountNumber)
                .build();
        bankAccount.setMapping(newMappingBuilder().withTarget(bankAccount).build());
        entities.getElements().add(bankAccount);

        // bank account to
        TransferObjectType mappedBankAccount = newTransferObjectTypeBuilder()
                .withName("BankAccount")
                .withAttributes(
                        newDataMemberBuilder()
                                .withName("accountNumber")
                                .withRequired(true)
                                .withDataType(string)
                                .withMemberType(MAPPED)
                                .withBinding(accountNumber)
                                .build())
                .withMapping(newMappingBuilder().withTarget(bankAccount).build())
                .build();
        admin.getElements().add(mappedBankAccount);

        // main bank account 0..1 association (2w)
        TwoWayRelationMember mainBankAccount = newTwoWayRelationMemberBuilder()
                .withName("mainBankAccount")
                .withTarget(bankAccount)
                .withLower(0).withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .withRangeType(DERIVED)
                .withRangeExpression("self.bankAccounts")
                .build();

        // bank account 0..* composition
        OneWayRelationMember bankAccounts = newOneWayRelationMemberBuilder()
                .withName("bankAccounts")
                .withTarget(bankAccount)
                .withLower(0).withUpper(-1)
                .withMemberType(STORED)
                .withRelationKind(COMPOSITION)
                .build();

        // name
        DataMember name = newDataMemberBuilder()
                .withName("name")
                .withRequired(true)
                .withDataType(string)
                .withMemberType(STORED)
                .build();

        // company entity
        EntityType company = newEntityTypeBuilder()
                .withName("Company")
                .withAttributes(name)
                .withRelations(mainBankAccount, bankAccounts)
                .build();
        company.setMapping(newMappingBuilder().withTarget(company).build());
        entities.getElements().add(company);

        // company to
        TransferObjectType mappedCompany = newTransferObjectTypeBuilder()
                .withName("Company")
                .withMapping(newMappingBuilder().withTarget(company).build())
                .withAttributes(
                        newDataMemberBuilder()
                                .withName("name")
                                .withRequired(true)
                                .withDataType(string)
                                .withMemberType(MAPPED)
                                .withBinding(name)
                                .build())
                .withRelations(
                        newOneWayRelationMemberBuilder()
                                .withName("bankAccounts")
                                .withTarget(bankAccount)
                                .withLower(0).withUpper(-1)
                                .withMemberType(MAPPED)
                                .withBinding(bankAccounts)
                                .withRelationKind(AGGREGATION)
                                .build(),
                        newOneWayRelationMemberBuilder()
                                .withName("mainBankAccount")
                                .withTarget(bankAccount)
                                .withLower(0).withUpper(1)
                                .withMemberType(MAPPED)
                                .withBinding(mainBankAccount)
                                .withRelationKind(AGGREGATION)
                                .withRangeType(DERIVED)
                                .withRangeExpression("self.bankAccounts")
                                .build())
                .build();
        admin.getElements().add(mappedCompany);

        // company 0..1 association (2w)
        TwoWayRelationMember toCompany = newTwoWayRelationMemberBuilder()
                .withName("company")
                .withTarget(company)
                .withLower(0).withUpper(1)
                .withMemberType(STORED)
                .withRelationKind(ASSOCIATION)
                .build();

        bankAccount.getRelations().add(toCompany);

        mainBankAccount.setPartner(toCompany);
        toCompany.setPartner(mainBankAccount);

        Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(measures, entities, admin)
                .build();

        // init dao
        daoFixture.init(model, datasourceFixture);
        assertTrue(daoFixture.isInitialized());

        AsmUtils asmUtils = daoFixture.getAsmUtils();

        EClass bankAccountClass = asmUtils.getClassByFQName(DTO_PACKAGE + ".BankAccount").get();
        EReference mainBankAccountReference = asmUtils.resolveReference(DTO_PACKAGE + ".Company#mainBankAccount").get();
        EReference companyReference = asmUtils.resolveReference(DTO_PACKAGE + ".BankAccount#company").get();

        EClass mappedCompanyClass = asmUtils.getClassByFQName(ADMIN_PACKAGE + ".Company").get();
        EReference mappedBankAccountsReference = asmUtils.resolveReference(ADMIN_PACKAGE + ".Company#bankAccounts").get();

        check(daoFixture, bankAccountClass, companyReference, mainBankAccountReference, new HashMap<>());

        UUID myCompanyId = daoFixture.getDao()
                .create(mappedCompanyClass, Payload.map("name", "MyCompany"),
                        DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idProviderClass, idProviderName);
        log.debug("Company created: [id={}, name={}]", myCompanyId, "MyCompany");

        UUID bankAccountId = daoFixture.getDao()
                .createNavigationInstanceAt(myCompanyId, mappedBankAccountsReference,
                                            Payload.map("accountNumber", "99999999"),
                                            DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idProviderClass, idProviderName);
        log.debug("BankAccount created: [id={}, accountNumber={}]", bankAccountId, "99999999");

        UUID bankAccount2Id = daoFixture.getDao()
                .createNavigationInstanceAt(myCompanyId, mappedBankAccountsReference,
                                            Payload.map("accountNumber", "00000000"),
                                            DAO.QueryCustomizer.<UUID>builder().build())
                .getAs(idProviderClass, idProviderName);
        log.debug("BankAccount created: [id={}, accountNumber={}]", bankAccount2Id, "00000000");

        check(daoFixture,
              bankAccountClass, companyReference, mainBankAccountReference,
              new HashMap<>() {{
                  put(bankAccountId, null);
                  put(bankAccount2Id, null);
              }}
        );

        Payload companyPayload = daoFixture.getDao().getByIdentifier(mappedCompanyClass, myCompanyId).get();
        companyPayload.remove("mainBankAccount");
        companyPayload.put("mainBankAccount", Payload.map(idProviderName, bankAccountId));
        daoFixture.getDao().update(mappedCompanyClass, companyPayload, DAO.QueryCustomizer.<UUID>builder().build());

        check(daoFixture,
              bankAccountClass, companyReference, mainBankAccountReference,
              new HashMap<>() {{
                  put(bankAccountId, myCompanyId);
                  put(bankAccount2Id, null);
              }}
        );

        Payload companyPayload2 = daoFixture.getDao().getByIdentifier(mappedCompanyClass, myCompanyId).get();
        companyPayload2.remove("mainBankAccount");
        companyPayload2.put("mainBankAccount", Payload.map(idProviderName, bankAccount2Id));
        daoFixture.getDao().update(mappedCompanyClass, companyPayload2, DAO.QueryCustomizer.<UUID>builder().build());

        check(daoFixture,
              bankAccountClass, companyReference, mainBankAccountReference,
              new HashMap<>() {{
                  put(bankAccountId, null);
                  put(bankAccount2Id, myCompanyId);
              }}
        );
    }

    /**
     * Checks if two way references have valid values
     *
     * @param daoFixture
     * @param referenceOwner         actual {@link EReference}'s owner
     * @param reference              {@link EReference} from actual reference owner's point of view
     * @param backReference          {@link EReference} from actual reference target's point of view
     * @param expectedReferencePairs {@link Map} of {@link UUID} pairs in which key is the actual reference owner and
     *                               value is the actual reference target
     */
    private void check(RdbmsDaoFixture daoFixture,
                       EClass referenceOwner, EReference reference, EReference backReference,
                       Map<UUID, UUID> expectedReferencePairs) {
        assertThat(daoFixture.getDao().getAllOf(referenceOwner).stream()
                           .map(p -> p.getAs(idProviderClass, idProviderName))
                           .collect(Collectors.toSet()),
                   equalTo(expectedReferencePairs.keySet()));
        expectedReferencePairs.forEach((ownerId, targetId) -> {
            List<Payload> referenceResult = daoFixture.getDao().getNavigationResultAt(ownerId, reference);
            if (targetId != null) {
                assertThat(referenceResult.size(), equalTo(1)); // single reference
                Payload referencePayload = referenceResult.get(0);
                assertThat(referencePayload, notNullValue());
                assertThat(referencePayload.getAs(idProviderClass, idProviderName), equalTo(targetId));

                List<Payload> backReferenceResult = daoFixture.getDao().getNavigationResultAt(targetId, backReference);
                assertThat(backReferenceResult.size(), equalTo(1)); // single reference
                Payload backReferencePayload = backReferenceResult.get(0);
                assertThat(backReferencePayload, notNullValue());
                assertThat(backReferencePayload.getAs(idProviderClass, idProviderName), equalTo(ownerId));
            } else {
                assertThat(referenceResult.size(), equalTo(0));
            }
        });
    }

}
