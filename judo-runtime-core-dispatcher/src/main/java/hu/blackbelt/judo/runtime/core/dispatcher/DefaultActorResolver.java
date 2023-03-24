package hu.blackbelt.judo.runtime.core.dispatcher;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.QueryCustomizerParameterProcessor;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.exception.AccessDeniedException;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DefaultActorResolver<ID> implements ActorResolver {

    DataTypeManager dataTypeManager;

    DAO<ID> dao;

    AsmModel asmModel;

    private Boolean checkMappedActors;

    private AsmUtils asmUtils;


    @Builder
    public DefaultActorResolver(
            @NonNull DataTypeManager dataTypeManager,
            @NonNull DAO<ID> dao,
            @NonNull AsmModel asmModel,
            Boolean checkMappedActors) {
        this.dataTypeManager = dataTypeManager;
        this.dao = dao;
        this.asmModel = asmModel;
        this.checkMappedActors = checkMappedActors == null ? false : checkMappedActors;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
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
        final EClass actorType = asmUtils.resolve(principal.getClient())
                .filter(a -> a instanceof EClass).map(a -> (EClass) a)
                .orElseThrow(() -> new IllegalStateException("Unsupported client"));

        if (asmUtils.isMappedTransferObjectType(actorType)) {
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
                    .filter(actorAttribute -> claims.containsKey(actorAttribute.getName()) 
                    		&& asmUtils.getMappedAttribute(actorAttribute).filter(a -> AsmUtils.isIdentifier(a)).isPresent())
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
            value = dataTypeManager.getCoercer().coerce(claims.get(filterAttribute.getName()), LocalDateTime.class);
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
            throw new AccessDeniedException(ValidationResult.builder()
                    .code("AUTHENTICATED_ENTITY_NOT_FOUND")
                    .level(ValidationResult.Level.ERROR)
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
