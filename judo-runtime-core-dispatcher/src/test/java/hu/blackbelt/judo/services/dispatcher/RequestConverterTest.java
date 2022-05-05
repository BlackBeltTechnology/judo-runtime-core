package hu.blackbelt.judo.services.dispatcher;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.epsilon.runtime.execution.impl.Slf4jLog;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.esm.namespace.Model;
import hu.blackbelt.judo.meta.esm.runtime.EsmModel;
import hu.blackbelt.judo.meta.esm.structure.MemberType;
import hu.blackbelt.judo.meta.esm.structure.RelationKind;
import hu.blackbelt.judo.meta.esm.structure.TransferObjectType;
import hu.blackbelt.judo.meta.esm.type.BooleanType;
import hu.blackbelt.judo.meta.esm.type.NumericType;
import hu.blackbelt.judo.meta.esm.type.StringType;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.services.core.exception.FeedbackItem;
import hu.blackbelt.judo.services.core.exception.ValidationException;
import hu.blackbelt.judo.services.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.services.dispatcher.validators.MaxLengthValidator;
import hu.blackbelt.judo.services.dispatcher.validators.PatternValidator;
import hu.blackbelt.judo.services.dispatcher.validators.PrecisionValidator;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkReport;
import hu.blackbelt.judo.tatami.core.workflow.work.WorkStatus;
import hu.blackbelt.judo.tatami.esm2psm.Esm2Psm;
import hu.blackbelt.judo.tatami.workflow.DefaultWorkflowSetupParameters;
import hu.blackbelt.judo.tatami.workflow.PsmDefaultWorkflow;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.meta.esm.namespace.util.builder.NamespaceBuilders.newModelBuilder;
import static hu.blackbelt.judo.meta.esm.structure.util.builder.StructureBuilders.*;
import static hu.blackbelt.judo.meta.esm.type.util.builder.TypeBuilders.*;
import static hu.blackbelt.judo.tatami.esm2psm.Esm2Psm.Esm2PsmParameter.esm2PsmParameter;
import static hu.blackbelt.judo.tatami.esm2psm.Esm2Psm.calculateEsm2PsmTransformationScriptURI;
import static hu.blackbelt.judo.tatami.esm2psm.Esm2Psm.executeEsm2PsmTransformation;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class RequestConverterTest {

    public static final String MODEL_NAME = "M";
    public static final String DTO_PACKAGE = MODEL_NAME + "._default_transferobjecttypes";

    private static final Coercer COERCER = new DefaultCoercer();

    @Test
    public void testSimpleRequest() throws Exception {
        final StringType licensePlate = newStringTypeBuilder().withName("String")
                .withMaxLength(255)
                .withRegExp("^[A-Z]{3}\\s*[0-9]{3}$")
                .build();
        final NumericType integerType = newNumericTypeBuilder().withName("Integer")
                .withPrecision(9)
                .withScale(0)
                .build();
        final BooleanType booleanType = newBooleanTypeBuilder().withName("Boolean")
                .build();

        final TransferObjectType testerTypeDefinition = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName("optionalLicensePlate")
                        .withDataType(licensePlate)
                        .withRequired(false)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("number")
                        .withDataType(integerType)
                        .withRequired(false)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("requiredLicensePlate")
                        .withDataType(licensePlate)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .build();
        useTransferObjectType(testerTypeDefinition)
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("optionalSingleTesterAggregation")
                        .withLower(0).withUpper(1)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("requiredSingleTesterAggregation")
                        .withLower(1).withUpper(1)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("optionalSingleTesterReference")
                        .withLower(0).withUpper(1)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("requiredSingleTesterReference")
                        .withLower(1).withUpper(1)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("optionalManyTesterAggregation")
                        .withLower(0).withUpper(2)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("requiredManyTesterAggregation")
                        .withLower(1).withUpper(2)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.AGGREGATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("optionalManyTesterReference")
                        .withLower(0).withUpper(2)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .withRelations(newOneWayRelationMemberBuilder()
                        .withName("requiredManyTesterReference")
                        .withLower(1).withUpper(2)
                        .withTarget(testerTypeDefinition)
                        .withMemberType(MemberType.TRANSIENT)
                        .withRelationKind(RelationKind.ASSOCIATION)
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(licensePlate, integerType, booleanType, testerTypeDefinition)
                .build();

        final AsmModel asmModel = getAsmModel(model);
        final AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        final EClass testerType = asmUtils.getClassByFQName("M.Tester").get();

        final Payload request = Payload.map(
                "optionalLicensePlate", "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "number", 1234567890,
                "optionalSingleTesterAggregation", Payload.empty(),
                "optionalSingleTesterReference", Payload.empty(),
                "optionalManyTesterAggregation", Arrays.asList(
                        Payload.empty(),
                        Payload.empty()
                )
        );

        final RequestConverter requestConverter = new RequestConverter(
                testerType,
                asmUtils,
                COERCER,
                null,
                false,
                RequestConverter.RequiredStringValidatorOption.ACCEPT_EMPTY,
                null,
                null,
                Arrays.asList(new MaxLengthValidator(), new PatternValidator(), new PrecisionValidator()),
                true,
                Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY));

        try {
            final Map<String, Object> validationContext = ImmutableMap.of(
                    RequestConverter.LOCATION_KEY, "test",
                    RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, false,
                    RequestConverter.VALIDATE_MISSING_FEATURES_KEY, true
            );
            requestConverter.convert(request, validationContext).get();
            fail("Validation error expected");
        } catch (ValidationException ex) {
            log.info("Validation items: {}", ex.getFeedbackItems().stream().map(vi -> "\n  - " + vi.toString()).collect(Collectors.joining()));
        }
    }

    @Test
    public void testRequiredStringValues() throws Exception {
        final StringType stringType = newStringTypeBuilder().withName("String")
                .withMaxLength(10)
                .build();

        final TransferObjectType testerTypeDefinition = newTransferObjectTypeBuilder()
                .withName("Tester")
                .withAttributes(newDataMemberBuilder()
                        .withName("missing")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("null")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("empty")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("trimmedEmpty")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .withAttributes(newDataMemberBuilder()
                        .withName("valid")
                        .withDataType(stringType)
                        .withRequired(true)
                        .withMemberType(MemberType.TRANSIENT)
                        .build())
                .build();

        final Model model = newModelBuilder()
                .withName(MODEL_NAME)
                .withElements(stringType, testerTypeDefinition)
                .build();

        final AsmModel asmModel = getAsmModel(model);
        final AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());

        final EClass testerType = asmUtils.getClassByFQName("M.Tester").get();

        final Payload payload = Payload.map(
                "null", null,
                "empty", "",
                "trimmedEmpty", "     ",
                "valid", "x"
        );

        final RequestConverter requestConverterMissing = new RequestConverter(
                testerType,
                asmUtils,
                COERCER,
                null,
                false,
                RequestConverter.RequiredStringValidatorOption.ACCEPT_EMPTY,
                null,
                null,
                Arrays.asList(new MaxLengthValidator(), new PatternValidator(), new PrecisionValidator()),
                true,
                Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY));
        final RequestConverter requestConverterEmpty = new RequestConverter(
                testerType,
                asmUtils,
                COERCER,
                null,
                false,
                RequestConverter.RequiredStringValidatorOption.ACCEPT_NON_EMPTY,
                null,
                null,
                Arrays.asList(new MaxLengthValidator(), new PatternValidator(), new PrecisionValidator()),
                true,
                Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY));
        final RequestConverter requestConverterTrimmedEmpty = new RequestConverter(
                testerType,
                asmUtils,
                COERCER,
                null,
                true,
                RequestConverter.RequiredStringValidatorOption.ACCEPT_NON_EMPTY,
                null,
                null,
                Arrays.asList(new MaxLengthValidator(), new PatternValidator(), new PrecisionValidator()),
                true,
                Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY));

        try {
            final Map<String, Object> validationContext = ImmutableMap.of(
                    RequestConverter.LOCATION_KEY, "test",
                    RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, false,
                    RequestConverter.VALIDATE_MISSING_FEATURES_KEY, true
            );
            requestConverterMissing.convert(payload, validationContext).get();
            fail("Validation error expected");
        } catch (ValidationException ex) {
            log.debug("Validation items: {}", ex.getFeedbackItems().stream().map(vi -> "\n  - " + vi.toString()).collect(Collectors.joining()));

            assertThat(ex.getFeedbackItems(), hasSize(2));
            assertTrue(ex.getFeedbackItems().stream().allMatch(f -> "MISSING_REQUIRED_ATTRIBUTE".equals(f.getCode())
                    && FeedbackItem.Level.ERROR.equals(f.getLevel())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/null".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/missing".equals(f.getLocation())));
        }

        try {
            final Map<String, Object> validationContext = ImmutableMap.of(
                    RequestConverter.LOCATION_KEY, "test",
                    RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, false,
                    RequestConverter.VALIDATE_MISSING_FEATURES_KEY, true
            );
            requestConverterEmpty.convert(payload, validationContext).get();
            fail("Validation error expected");
        } catch (ValidationException ex) {
            log.debug("Validation items: {}", ex.getFeedbackItems().stream().map(vi -> "\n  - " + vi.toString()).collect(Collectors.joining()));

            assertThat(ex.getFeedbackItems(), hasSize(3));
            assertTrue(ex.getFeedbackItems().stream().allMatch(f -> "MISSING_REQUIRED_ATTRIBUTE".equals(f.getCode())
                    && FeedbackItem.Level.ERROR.equals(f.getLevel())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/null".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/missing".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/empty".equals(f.getLocation())));
        }

        try {
            final Map<String, Object> validationContext = ImmutableMap.of(
                    RequestConverter.LOCATION_KEY, "test",
                    RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, false,
                    RequestConverter.VALIDATE_MISSING_FEATURES_KEY, true
            );
            requestConverterTrimmedEmpty.convert(payload, validationContext).get();
            fail("Validation error expected");
        } catch (ValidationException ex) {
            log.debug("Validation items: {}", ex.getFeedbackItems().stream().map(vi -> "\n  - " + vi.toString()).collect(Collectors.joining()));

            assertThat(ex.getFeedbackItems(), hasSize(4));
            assertTrue(ex.getFeedbackItems().stream().allMatch(f -> "MISSING_REQUIRED_ATTRIBUTE".equals(f.getCode())
                    && FeedbackItem.Level.ERROR.equals(f.getLevel())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/null".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/missing".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/empty".equals(f.getLocation())));
            assertTrue(ex.getFeedbackItems().stream().anyMatch(f -> "test/trimmedEmpty".equals(f.getLocation())));
        }
    }

    private AsmModel getAsmModel(Model model) throws Exception {
        final EsmModel esmModel = createEsmModel(model);

        java.lang.String createdSourceModelName = "urn:psm.judo-meta-psm";
        PsmModel psmModel = PsmModel.buildPsmModel().uri(URI.createURI(createdSourceModelName)).name("demo").build();

        executeEsm2PsmTransformation(esm2PsmParameter().esmModel(esmModel).psmModel(psmModel));

        final PsmDefaultWorkflow defaultWorkflow = new PsmDefaultWorkflow(
                DefaultWorkflowSetupParameters.defaultWorkflowSetupParameters()
                        .psmModel(psmModel)
                        .modelName(model.getName())
                        .ignoreAsm2Rdbms(true)
                        .ignoreRdbms2Liquibase(true)
                        .ignoreAsm2Expression(true)
                        .ignoreAsm2jaxrsapi(true)
                        .ignoreAsm2Openapi(true)
                        .ignoreAsm2sdk(true)
                        .ignoreAsm2Keycloak(true)
                        .ignoreScript2Operation(true)
                        .validateModels(false)
                        .dialectList(Collections.emptyList()));
        final WorkReport workReport = defaultWorkflow.startDefaultWorkflow();
        assertEquals(WorkStatus.COMPLETED, workReport.getStatus());

        return defaultWorkflow.getTransformationContext().getByClass(AsmModel.class).get();
    }

    private EsmModel createEsmModel(Model model) {
        java.lang.String createdSourceModelName = "urn:esm.judo-meta-esm";
        EsmModel esmModel = EsmModel.buildEsmModel().uri(URI.createURI(createdSourceModelName)).name("demo").build();
        esmModel.addContent(model);
        return esmModel;
    }
}
