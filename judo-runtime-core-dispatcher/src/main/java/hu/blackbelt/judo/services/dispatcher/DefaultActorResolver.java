package hu.blackbelt.judo.services.dispatcher;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.core.DataTypeManager;
import hu.blackbelt.judo.services.dispatcher.behaviours.QueryCustomizerParameterProcessor;
import hu.blackbelt.judo.services.core.exception.AccessDeniedException;
import hu.blackbelt.judo.services.core.exception.FeedbackItem;
import hu.blackbelt.judo.services.dispatcher.security.ActorResolver;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class DefaultActorResolver<ID> implements ActorResolver {

    @NonNull
    DataTypeManager dataTypeManager;

    @NonNull
    DAO<ID> dao;

    @NonNull
    AsmModel asmModel;

    @NonNull
    private boolean checkMappedActors;

    private AsmUtils asmUtils;


    private AsmUtils getAsmUtils() {
        if (asmUtils == null) {
            asmUtils = new AsmUtils(asmModel.getResourceSet());
        }
        return asmUtils;
    }

    @Override
    public void authenticateActor(final Map<String, Object> exchange) {
        final Object principal = exchange.get(Dispatcher.PRINCIPAL_KEY);

        if ((principal instanceof JudoPrincipal) && !exchange.containsKey(Dispatcher.ACTOR_KEY) && checkMappedActors) {
            final Optional<Payload> actor = authenticateByPrincipal((JudoPrincipal) principal);
            actor.ifPresent(a -> exchange.put(Dispatcher.ACTOR_KEY, a));
        }
    }

    @Override
    public Optional<Payload> authenticateByPrincipal(JudoPrincipal principal) {
        final EClass actorType = getAsmUtils().resolve(principal.getClient())
                .filter(a -> a instanceof EClass).map(a -> (EClass) a)
                .orElseThrow(() -> new IllegalStateException("Unsupported client"));

        if (getAsmUtils().isMappedTransferObjectType(actorType)) {
            final Map<String, Object> claims = principal.getAttributes().entrySet().stream()
                    .filter(e -> e.getValue() instanceof String)
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

            return Optional.of(getActorByClaims(actorType, claims));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Payload getActorByClaims(final EClass actorType, final Map<String, Object> claims) {
        final Optional<EAttribute> usernameClaim = actorType.getEAllAttributes().stream()
                .filter(claim -> Objects.equals(AsmUtils.getExtensionAnnotationValue(claim, "claim", false).orElse("-"), "USERNAME"))
                .findAny();
        final Optional<EAttribute> emailClaim = actorType.getEAllAttributes().stream()
                .filter(claim -> Objects.equals(AsmUtils.getExtensionAnnotationValue(claim, "claim", false).orElse("-"), "EMAIL"))
                .findAny();

        final EAttribute filterAttribute;
        if (usernameClaim.isPresent() && claims.get(usernameClaim.get().getName()) != null) {
            filterAttribute = usernameClaim.get();
        } else if (emailClaim.isPresent() && claims.get(emailClaim.get().getName()) != null) {
            filterAttribute = emailClaim.get();
        } else {
            filterAttribute = actorType.getEAllAttributes().stream()
                    .filter(actorAttribute -> claims.containsKey(actorAttribute.getName()) && getAsmUtils().getMappedAttribute(actorAttribute).filter(a -> getAsmUtils().isIdentifier(a)).isPresent())
                    .sorted((a1, a2) -> AsmUtils.equals(a1, a2) ? 0 : a1.getName().compareTo(a2.getName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("ID attribute in Entity type matching any access token claim not found"));
        }

        final Integer operator;
        final Object value;
        if (AsmUtils.isDecimal(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), BigDecimal.class);
        } else if (AsmUtils.isInteger(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), BigInteger.class);
        } else if (AsmUtils.isDate(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), LocalDate.class);
        } else if (AsmUtils.isTimestamp(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), OffsetDateTime.class);
        } else if (AsmUtils.isTime(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), LocalTime.class);
        } else if (AsmUtils.isBoolean(filterAttribute.getEAttributeType())) {
            operator = 0;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), Boolean.class);
        } else if (AsmUtils.isString(filterAttribute.getEAttributeType()) || AsmUtils.isText(filterAttribute.getEAttributeType())) {
            operator = 4;
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), String.class);
        } else if (AsmUtils.isEnumeration(filterAttribute.getEAttributeType())) {
            operator = 0;
            final String literal = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), String.class);
            value = ((EEnum) filterAttribute.getEAttributeType()).getELiterals().stream()
                    .filter(l -> Objects.equals(l.getLiteral(), literal))
                    .map(l -> l.getValue())
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Invalid enumeration literal: " + literal));
        } else {
            log.warn("Unsupported filtering type: {}", AsmUtils.getAttributeFQName(filterAttribute));
            operator = null;
            value = null;
        }

        final String filter;
        if (operator != null) {
            filter = QueryCustomizerParameterProcessor.convertFilterToJql(filterAttribute, operator, value, false);
        } else {
            filter = null;
        }
        final List<Payload> queryResult = dao.search(actorType, DAO.QueryCustomizer.<ID>builder()
                .filter(filter)
                .seek(DAO.Seek.builder()
                        .limit(1)
                        .build())
                .build());

        if (queryResult.isEmpty()) {
            log.info("Operation failed, authenticated entity not found in database");
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put("ACTOR", AsmUtils.getClassifierFQName(actorType));
            throw new AccessDeniedException(FeedbackItem.builder()
                    .code("AUTHENTICATED_ENTITY_NOT_FOUND")
                    .level(FeedbackItem.Level.ERROR)
                    .details(details)
                    .build());
        }

        if (queryResult.size() > 1) {
            throw new SecurityException("Multiple actors found in database by token");
        }

        final Payload result = queryResult.get(0);
        // add transient attributes (claims) from Access Token
        result.putAll(claims.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .filter(e -> actorType.getEAllAttributes().stream().anyMatch(a -> Objects.equals(a.getName(), e.getKey()) && AsmUtils.annotatedAsTrue(a, "transient")))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));

        return result;
    }
}
