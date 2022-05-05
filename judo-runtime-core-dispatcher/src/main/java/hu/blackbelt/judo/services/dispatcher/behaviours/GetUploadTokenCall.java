package hu.blackbelt.judo.services.dispatcher.behaviours;

import com.google.gson.Gson;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.osgi.filestore.security.api.Token;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.UploadClaim;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EOperation;

import java.util.Map;
import java.util.TreeMap;

public class GetUploadTokenCall implements BehaviourCall {

    private static final String TOKEN_KEY = "token";
    public static final String ATTRIBUTE_KEY = "attribute";

    private final AsmUtils asmUtils;

    private TokenIssuer tokenIssuer;

    public GetUploadTokenCall(final AsmUtils asmUtils, final TokenIssuer tokenIssuer) {
        this.asmUtils = asmUtils;
        this.tokenIssuer = tokenIssuer;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_UPLOAD_TOKEN).isPresent();
    }

    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {
        final EAttribute owner = (EAttribute) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final Map<String, Object> context = new TreeMap<>();
        context.put(ATTRIBUTE_KEY, AsmUtils.getAttributeFQName(owner));

        final String uploadTokenString = tokenIssuer.createUploadToken(Token.<UploadClaim>builder()
                .jwtClaim(UploadClaim.FILE_MIME_TYPE_LIST, AsmUtils.getExtensionAnnotationCustomValue(owner.getEAttributeType(), "constraints", "mimeTypes", false).orElse(null))
                .jwtClaim(UploadClaim.MAX_FILE_SIZE, AsmUtils.getExtensionAnnotationCustomValue(owner.getEAttributeType(), "constraints", "maxFileSize", false).map(v -> Long.parseLong(v)).orElse(null))
                .jwtClaim(UploadClaim.CONTEXT, new Gson().toJson(context))
                .build());

        return Payload.map(TOKEN_KEY, uploadTokenString);
    }
}
