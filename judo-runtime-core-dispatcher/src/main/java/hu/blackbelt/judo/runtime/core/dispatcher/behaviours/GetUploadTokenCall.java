package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.osgi.filestore.security.api.Token;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.UploadClaim;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EOperation;

import java.util.Map;
import java.util.TreeMap;

public class GetUploadTokenCall<ID> implements BehaviourCall<ID> {

    private static final String TOKEN_KEY = "token";
    public static final String ATTRIBUTE_KEY = "attribute";

    private final AsmModel asmModel;

    private final AsmUtils asmUtils;

    private final TokenIssuer tokenIssuer;

    private final OperationCallInterceptorProvider interceptorProvider;

    public GetUploadTokenCall(final AsmModel asmModel, final TokenIssuer tokenIssuer, OperationCallInterceptorProvider interceptorProvider) {
        this.asmModel = asmModel;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.tokenIssuer = tokenIssuer;
        this.interceptorProvider = interceptorProvider;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_UPLOAD_TOKEN).isPresent();
    }

    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {

        CallInterceptorUtil<GetUploadTokenCallPayload, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                GetUploadTokenCallPayload.class, Payload.class, asmModel, operation, interceptorProvider
        );

        GetUploadTokenCallPayload inputParameter = callInterceptorUtil.preCallInterceptors(
                GetUploadTokenCallPayload.builder()
                        .instance(Payload.asPayload(exchange))
                        .owner((EAttribute) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation).orElseThrow(
                                () -> new IllegalArgumentException("Invalid model")))
                        .build());

        Payload result = null;

        if (callInterceptorUtil.isOriginalCalled()) {
            final Map<String, Object> context = new TreeMap<>();
            context.put(ATTRIBUTE_KEY, AsmUtils.getAttributeFQName(inputParameter.getOwner()));

            final String uploadTokenString = tokenIssuer.createUploadToken(Token.<UploadClaim>builder()
                    .jwtClaim(UploadClaim.FILE_MIME_TYPE_LIST, AsmUtils.getExtensionAnnotationCustomValue(inputParameter.getOwner().getEAttributeType(), "constraints", "mimeTypes", false).orElse(null))
                    .jwtClaim(UploadClaim.MAX_FILE_SIZE, AsmUtils.getExtensionAnnotationCustomValue(inputParameter.getOwner().getEAttributeType(), "constraints", "maxFileSize", false).map(Long::parseLong).orElse(null))
                    .jwtClaim(UploadClaim.CONTEXT, new Gson().toJson(context))
                    .build());

            result = Payload.map(TOKEN_KEY, uploadTokenString);
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class GetUploadTokenCallPayload {
        @NonNull
        EAttribute owner;

        @NonNull
        Payload instance;
    }

}
