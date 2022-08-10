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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.exception.AccessDeniedException;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jwk.*;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.EllipticCurves;
import org.jose4j.keys.HmacKey;
import org.jose4j.lang.JoseException;

import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.ECParameterSpec;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
@Builder
public class DefaultIdentifierSigner<ID> implements IdentifierSigner {

    private static final String ENTITY_CLAIM = "entity";
    private static final String ENTITY_VERSION = "ver";
    private static final String IMMUTABLE = "immutable";
    private static final int GENERATED_RSA_KEY_SIZE = 2048;
    private static final ECParameterSpec GENERATED_EC_KEY_SPEC = EllipticCurves.P521;
    private static final int GENERATED_SECRET_KEY_SIZE = 1024;

    private static final String VERSION_KEY = "__version";

    @NonNull
    @Setter
    AsmModel asmModel;

    @NonNull
    @Setter
    IdentifierProvider<ID> identifierProvider;

    @NonNull
    @Setter
    DataTypeManager dataTypeManager;

    private AsmUtils asmUtils;

    private Key privateKey;
    private Key publicKey;

    @Builder.Default
    String algorithm = AlgorithmIdentifiers.HMAC_SHA512;

    @Setter
    String secret;

    @Setter
    String keys;

    protected void configureKeys() {
        asmUtils = new AsmUtils(asmModel.getResourceSet());
        if (algorithm.startsWith("HS")) {
            loadHMACKey();
        } else if (algorithm.startsWith("RS")) {
            loadRSAKey();
        } else if (algorithm.startsWith("ES")) {
            loadECKey();
        } else if (algorithm.startsWith("PS")) {
            loadRSAKey();
        } else if (!AlgorithmIdentifiers.NONE.equals(algorithm)) {
            throw new UnsupportedOperationException("Unsupported JWT algorithm: " + algorithm);
        }
    }

    private void loadRSAKey() {
        try {
            if (keys != null && !"".equals(keys)) {
                log.info("Loading RSA key pair...");
                final PublicJsonWebKey jsonWebKey = PublicJsonWebKey.Factory.newPublicJwk(new String(Base64.getDecoder().decode(keys)));
                privateKey = jsonWebKey.getPrivateKey();
                publicKey = jsonWebKey.getPublicKey();
            } else {
                log.info("Generating RSA key pair...");
                final RsaJsonWebKey rsaJsonWebKey = RsaJwkGenerator.generateJwk(GENERATED_RSA_KEY_SIZE);
                if (log.isTraceEnabled()) {
                    log.trace("Generated RSA key: {}", rsaJsonWebKey.toJson(JsonWebKey.OutputControlLevel.INCLUDE_PRIVATE));
                }
                privateKey = rsaJsonWebKey.getPrivateKey();
                publicKey = rsaJsonWebKey.getPublicKey();
            }
        } catch (JoseException ex) {
            throw new IllegalStateException("Unable to initialize RSA key", ex);
        }
    }

    private void loadECKey() {
        try {
            if (keys != null && !"".equals(keys)) {
                log.info("Loading EC key pair...");
                final PublicJsonWebKey jsonWebKey = PublicJsonWebKey.Factory.newPublicJwk(new String(Base64.getDecoder().decode(keys)));
                privateKey = jsonWebKey.getPrivateKey();
                publicKey = jsonWebKey.getPublicKey();
            } else {
                log.info("Generating EC key pair...");
                final EllipticCurveJsonWebKey ellipticCurveJsonWebKey = EcJwkGenerator.generateJwk(GENERATED_EC_KEY_SPEC);
                if (log.isTraceEnabled()) {
                    log.trace("Generated EC key: {}", ellipticCurveJsonWebKey.toJson(JsonWebKey.OutputControlLevel.INCLUDE_PRIVATE));
                }
                privateKey = ellipticCurveJsonWebKey.getEcPrivateKey();
                publicKey = ellipticCurveJsonWebKey.getECPublicKey();
            }
        } catch (JoseException ex) {
            throw new IllegalStateException("Unable to initialize EC key", ex);
        }
    }

    private void loadHMACKey() {
        if (secret != null && !"".equals(secret)) {
            log.info("Loading HMAC secret...");
            privateKey = new HmacKey(Base64.getDecoder().decode(secret));
            publicKey = privateKey;
        } else {
            try {
                log.info("Generating HMAC secret...");
                final SecureRandom random = SecureRandom.getInstanceStrong();
                final byte[] values = new byte[GENERATED_SECRET_KEY_SIZE / 8];
                random.nextBytes(values);
                if (log.isTraceEnabled()) {
                    log.trace("Generated secret: {}", Base64.getEncoder().encodeToString(values));
                }
                privateKey = new HmacKey(values);
                publicKey = privateKey;
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalStateException("Unable to initialize identifier signer");
            }
        }
    }


    @SuppressWarnings("unchecked")
	@Override
    public void signIdentifiers(final ETypedElement typedElement, final Map<String, Object> payload, final boolean immutable) {
        if (privateKey == null) {
            configureKeys();
        }
        if (typedElement.getEType() instanceof EClass) {
			final ID id = (ID) payload.get(identifierProvider.getName());
            final String entityType = (String) payload.get(Dispatcher.ENTITY_TYPE_MAP_KEY);
            final Integer version = (Integer) payload.get(VERSION_KEY);
            if (asmUtils.isMappedTransferObjectType((EClass) typedElement.getEType()) && id != null) {
                payload.put(SIGNED_IDENTIFIER_KEY, sign(typedElement, id, entityType, version, immutable));

                final boolean updateable;
                final boolean deleteable;

                final Optional<EAnnotation> permissions = AsmUtils.getExtensionAnnotationByName(typedElement, "permissions", false);
                if (permissions.isPresent()) {
                    updateable = Boolean.parseBoolean(permissions.get().getDetails().get("update"));
                    deleteable = Boolean.parseBoolean(permissions.get().getDetails().get("delete"));
                } else {
                    updateable = false;
                    deleteable = false;
                }

                payload.put(DefaultDispatcher.UPDATEABLE_KEY, updateable);
                payload.put(DefaultDispatcher.DELETEABLE_KEY, deleteable);
                if (immutable) {
                    payload.put(DefaultDispatcher.IMMUTABLE_KEY, true);
                }
            }

            ((EClass) typedElement.getEType()).getEAllReferences().stream()
                    .filter(reference -> payload.get(reference.getName()) != null)
                    .forEach(reference -> {
                        if (reference.isMany()) {
                            ((Collection<Map<String, Object>>) payload.get(reference.getName()))
                                    .forEach(containment -> signIdentifiers(reference, containment, immutable));
                        } else {
                            signIdentifiers(reference, (Map<String, Object>) payload.get(reference.getName()), immutable);
                        }
                    });
        }
    }

    @Override
    public Optional<SignedIdentifier> extractSignedIdentifier(final EClass clazz, final Map<String, Object> payload) {
        if (privateKey == null) {
            configureKeys();
        }

        final String signedIdentifierAsString = (String) payload.get(SIGNED_IDENTIFIER_KEY);

        if (asmUtils.isMappedTransferObjectType(clazz) && signedIdentifierAsString != null) {
            final SignedIdentifier signedIdentifier = verify(signedIdentifierAsString);

            final Object originalId = payload.get(identifierProvider.getName());
            if (originalId != null) {
                checkArgument(Objects.equals(dataTypeManager.getCoercer().coerce(originalId, String.class), signedIdentifier.getIdentifier()));
                if (payload.containsKey(Dispatcher.ENTITY_TYPE_MAP_KEY) && signedIdentifier.getEntityType() != null) {
                    checkArgument(Objects.equals(payload.get(Dispatcher.ENTITY_TYPE_MAP_KEY), signedIdentifier.getEntityType()));
                }
                if (!identifierProvider.getType().isAssignableFrom(originalId.getClass())) {
                    payload.put(identifierProvider.getName(), dataTypeManager.getCoercer().coerce(originalId, identifierProvider.getType()));
                }
                if (payload.containsKey(VERSION_KEY) && signedIdentifier.getVersion() != null) {
                    checkArgument(Objects.equals(payload.get(VERSION_KEY), signedIdentifier.getVersion()));
                }
            } else {
                final ID id = dataTypeManager.getCoercer().coerce(signedIdentifier.getIdentifier(), identifierProvider.getType());
                payload.put(identifierProvider.getName(), id);
                payload.put(Dispatcher.ENTITY_TYPE_MAP_KEY, signedIdentifier.getEntityType());
                if (signedIdentifier.getVersion() != null) {
                    payload.put(VERSION_KEY, signedIdentifier.getVersion());
                }
            }
            checkArgument(signedIdentifier.getProducedBy() != null, "Unable to check source of data");

            if (!hasCorrectSigner((EClass) signedIdentifier.getProducedBy().getEType(), clazz)) {
                log.info("Mapped transfer object type {} does not match type of signed identifier {}", AsmUtils.getClassifierFQName(clazz), AsmUtils.getClassifierFQName(signedIdentifier.getProducedBy().getEType()));
                throw new AccessDeniedException(ValidationResult.builder()
                        .code("ACCESS_DENIED_INVALID_TYPE")
                        .level(ValidationResult.Level.ERROR)
                        .build());
            }

            return Optional.of(signedIdentifier);
        } else {
            checkArgument(payload.get(identifierProvider.getName()) == null, "Identifier is not allowed without signed identifier");

            return Optional.empty();
        }
    }

    private EClass getOverrideOfClass(EClass clazz) {
        return AsmUtils.getExtensionAnnotationValue(clazz, "override", false)
                .map(fqName -> asmUtils.resolve(fqName).filter(c -> c instanceof EClass).map(c -> (EClass) c).orElse(null))
                .orElse(null);
    }

    private boolean hasCorrectSigner(EClass signerClass, EClass accessedClass) {
        final EClass accessedClassOverride = getOverrideOfClass(accessedClass);
        final EClass signerClassOverride = getOverrideOfClass(signerClass);

        return AsmUtils.equals(signerClass, accessedClass) ||
                AsmUtils.equals(signerClass, accessedClassOverride) ||
                AsmUtils.equals(signerClassOverride, accessedClass) ||
                signerClass.getEAllSuperTypes().contains(accessedClass) ||
                (accessedClassOverride != null && signerClass.getEAllSuperTypes().contains(accessedClassOverride));
    }

    private String sign(final ETypedElement typedElement, final ID id, final String entityType, final Integer version, final Boolean immutable) {
        final String idAsString = dataTypeManager.getCoercer().coerce(id, String.class);

        final JwtClaims claims = new JwtClaims();
        claims.setSubject(idAsString);
        claims.setIssuedAtToNow();
        claims.setClaim(ENTITY_CLAIM, entityType);
        if (version != null) {
            claims.setClaim(ENTITY_VERSION, version);
        }
        if (Boolean.TRUE.equals(immutable)) {
            claims.setClaim(IMMUTABLE, true);
        }
        if (typedElement instanceof EReference) {
            claims.setIssuer(AsmUtils.getReferenceFQName((EReference) typedElement));
        } else if (typedElement instanceof EOperation) {
            claims.setIssuer(AsmUtils.getOperationFQName((EOperation) typedElement));
        }

        final JsonWebSignature jws = new JsonWebSignature();
        jws.setKey(privateKey);
        jws.setPayload(claims.toJson());
        jws.setAlgorithmHeaderValue(algorithm);

        try {
            return jws.getCompactSerialization();
        } catch (JoseException ex) {
            throw new IllegalStateException("Unable to sign identifier", ex);
        }
    }

    private SignedIdentifier verify(final String signedIdentifierAsString) {
        final JwtConsumer jwtConsumer = new JwtConsumerBuilder()
                .setRelaxVerificationKeyValidation()
                .setRequireSubject()
                .setVerificationKey(publicKey)
                .setJwsAlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, algorithm)
                .build();

        try {
            final JwtClaims jwtClaims = jwtConsumer.processToClaims(signedIdentifierAsString);

            final String issuer = jwtClaims.getIssuer();
            final Optional<ETypedElement> issuerOperation = asmUtils.resolveOperation(issuer).map(operation -> operation);
            final Optional<ETypedElement> issuerReference = asmUtils.resolveReference(issuer).map(reference -> reference);
            final String entityType = jwtClaims.getClaimValue(ENTITY_CLAIM, String.class);
            final String version = jwtClaims.getClaimValueAsString(ENTITY_VERSION);
            final Boolean immutable = jwtClaims.getClaimValue(IMMUTABLE, Boolean.class);

            return SignedIdentifier.builder()
                    .identifier(jwtClaims.getSubject())
                    .producedBy(issuerOperation.orElse(issuerReference.orElse(null)))
                    .entityType(entityType)
                    .version(version != null ? Integer.parseInt(version) : null)
                    .immutable(immutable)
                    .build();
        } catch (InvalidJwtException | MalformedClaimException e) {
            throw new IllegalStateException("Invalid signed identifier", e);
        }
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        final SecureRandom random = SecureRandom.getInstanceStrong();
        final byte[] values = new byte[GENERATED_SECRET_KEY_SIZE / 8];
        random.nextBytes(values);
        System.out.println("Generated secret: " + Base64.getEncoder().encodeToString(values));
    }
}
