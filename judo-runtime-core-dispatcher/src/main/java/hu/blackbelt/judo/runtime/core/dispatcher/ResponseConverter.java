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

import com.google.gson.Gson;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.PayloadTraverser;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.GetUploadTokenCall;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.osgi.filestore.security.api.DownloadClaim;
import hu.blackbelt.osgi.filestore.security.api.Token;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;

import java.util.*;

@Builder
public class ResponseConverter {

    @NonNull
    private final EClass transferObjectType;

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final Coercer coercer;

    private final TokenIssuer filestoreTokenIssuer;

    @NonNull
    @Singular
    private final Collection<String> keepProperties;

    public Optional<Payload> convert(final Map<String, Object> input) {
        if (input == null) {
            return Optional.empty();
        }

        return Optional.of(PayloadTraverser.builder()
                .predicate((reference) -> AsmUtils.isEmbedded(reference))
                .processor((instance, ctx) -> {
                    ctx.getType().getEAllAttributes().stream()
                            .filter(a -> instance.get(a.getName()) != null) // missing and null values are not touched
                            .forEach(a -> instance.put(a.getName(), a.getEAttributeType() instanceof EEnum
                                    ? convertEnumerationValue(a.getEAttributeType(), (Integer) instance.get(a.getName()))
                                    : convertNonEnumerationValue(a, instance.get(a.getName()))));
                    for (final Iterator<Map.Entry<String, Object>> it = instance.entrySet().iterator(); it.hasNext(); ) {
                        final Map.Entry<String, Object> entry = it.next();
                        if (!ctx.getType().getEAllStructuralFeatures().stream()
                                .filter(f -> !ctx.getType().getEAllStructuralFeatures().stream().anyMatch(d -> Objects.equals(f.getName(), AsmUtils.getExtensionAnnotationValue(d, "default", false).orElse("-"))))
                                .anyMatch(f -> Objects.equals(f.getName(), entry.getKey()))
                                && !keepProperties.contains(entry.getKey())
                                || entry.getValue() == null) {
                            it.remove();
                        }
                    }
                })
                .build()
                .traverse(Payload.asPayload(input), transferObjectType));
    }

    private final Object convertEnumerationValue(final EDataType dataType, final Integer oldValue) {
        return Optional.ofNullable(asmUtils.all(EEnum.class)
                        .filter(e -> AsmUtils.equals(e, dataType))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Invalid enumeration type: " + AsmUtils.getClassifierFQName(dataType)))
                        .getEEnumLiteral(oldValue))
                .map(l -> l.getLiteral())
                .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration value '" + oldValue + "' of type: " + AsmUtils.getClassifierFQName(dataType)));
    }

    @SuppressWarnings("unchecked")
    private final Object convertNonEnumerationValue(final EAttribute attribute, final Object oldValue) {
        if (AsmUtils.isByteArray(attribute.getEAttributeType()) && oldValue != null) {
            final Map<String, Object> context = new TreeMap<>();
            context.put(GetUploadTokenCall.ATTRIBUTE_KEY, AsmUtils.getAttributeFQName(attribute));
            final String contextString = new Gson().toJson(context);

            if (oldValue instanceof Token) {
                return filestoreTokenIssuer.createDownloadToken((Token<DownloadClaim>) oldValue);
            } else if (oldValue instanceof Map) {
                return filestoreTokenIssuer.createDownloadToken(Token.<DownloadClaim>builder()
                        .jwtClaim(DownloadClaim.FILE_ID, ((Map<String, Object>) oldValue).get(DownloadClaim.FILE_ID.getJwtClaimName()))
                        .jwtClaim(DownloadClaim.FILE_NAME, ((Map<String, Object>) oldValue).get(DownloadClaim.FILE_NAME.getJwtClaimName()))
                        .jwtClaim(DownloadClaim.FILE_SIZE, ((Map<String, Object>) oldValue).get(DownloadClaim.FILE_SIZE.getJwtClaimName()))
                        .jwtClaim(DownloadClaim.FILE_MIME_TYPE, ((Map<String, Object>) oldValue).get(DownloadClaim.FILE_MIME_TYPE.getJwtClaimName()))
                        .jwtClaim(DownloadClaim.CONTEXT, contextString)
                        .jwtClaim(DownloadClaim.DISPOSITION, "attachment")
                        .build());
            } else if (oldValue instanceof FileType) {
                return filestoreTokenIssuer.createDownloadToken(Token.<DownloadClaim>builder()
                        .jwtClaim(DownloadClaim.FILE_ID, ((FileType) oldValue).getId())
                        .jwtClaim(DownloadClaim.FILE_NAME, ((FileType) oldValue).getFileName())
                        .jwtClaim(DownloadClaim.FILE_SIZE, ((FileType) oldValue).getSize())
                        .jwtClaim(DownloadClaim.FILE_MIME_TYPE, ((FileType) oldValue).getMimeType())
                        .jwtClaim(DownloadClaim.CONTEXT, contextString)
                        .jwtClaim(DownloadClaim.DISPOSITION, "attachment")
                        .build());
            } else {
                throw new IllegalStateException("Unknown binary data format: " + oldValue.getClass().getName());
            }
        } else {
            return coercer.coerce(oldValue, attribute.getEAttributeType().getInstanceClassName());
        }
    }
}
