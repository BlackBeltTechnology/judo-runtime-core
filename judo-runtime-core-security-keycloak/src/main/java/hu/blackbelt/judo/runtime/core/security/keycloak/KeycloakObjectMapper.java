package hu.blackbelt.judo.runtime.core.security.keycloak;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import hu.blackbelt.judo.meta.keycloak.*;
import hu.blackbelt.judo.meta.keycloak.impl.*;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

import java.io.IOException;

public class KeycloakObjectMapper {

    private static class EListDeserializer extends JsonDeserializer<EList<?>> implements ContextualDeserializer {

        private JavaType valueType;

        @Override
        public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
            JavaType valueType = property.getType().containedType(0);
            EListDeserializer deserializer = new EListDeserializer();
            deserializer.valueType = valueType;
            return deserializer;
        }

        @Override
        public EList<?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);
            EList<?> elist = new BasicEList<>();
            if(node.isArray()) {
                ObjectMapper mapper = KeycloakObjectMapper.objectMapper();
                for (JsonNode objNode : node) {
                        elist.add(mapper.readValue(objNode.traverse(jsonParser.getCodec()), valueType));
                }
            }
            return elist;
        }
    }

    public static class ClientImplWithCollectionSetters extends ClientImpl {
        public void setRedirectUris(EList<String> eList) {
            this.getRedirectUris().clear();
            this.getRedirectUris().addAll(eList);
        }

        public void setAttributeBindings(EList<AttributeBinding> eList) {
            this.getAttributeBindings().clear();
            this.getAttributeBindings().addAll(eList);
        }

        public void setWebOrigins(EList<String> eList) {
            this.getWebOrigins().clear();
            this.getWebOrigins().addAll(eList);
        }
    }

    public static class UserImplWithCollectionSetters extends UserImpl {
        public void setCredentials(EList<UserCredential> eList) {
            this.getCredentials().clear();
            this.getCredentials().addAll(eList);
        }

        public void setRequiredActions(EList<String> eList) {
            this.getRequiredActions().clear();
            this.getRequiredActions().addAll(eList);
        }
    }

    public static class RealmImplWithCollectionSetters extends RealmImpl {
        public void setUsers(EList<User> eList) {
            this.getUsers().clear();
            this.getUsers().addAll(eList);
        }
        public void setClients(EList<Client> eList) {
            this.getClients().clear();
            this.getClients().addAll(eList);
        }
    }

    @JsonDeserialize(as = ClientImplWithCollectionSetters.class)
    private abstract static class ClientMixIn {
    }

    @JsonDeserialize(as = UserImplWithCollectionSetters.class)
    private abstract static class UserMixIn {
    }

    @JsonDeserialize(as = RealmImplWithCollectionSetters.class)
    private abstract static class RealmMixIn {
    }

    @JsonDeserialize(as = AttributeBindingImpl.class)
    private abstract static class AttributeBindingMixIn {
    }

    @JsonDeserialize(as = UserCredentialImpl.class)
    private abstract static class UserCredentialMixIn {
    }

    public static ObjectMapper objectMapper() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(EList.class, new EListDeserializer());
        final ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(Client.class, ClientMixIn.class);
        mapper.addMixIn(User.class, UserMixIn.class);
        mapper.addMixIn(Realm.class, RealmMixIn.class);
        mapper.addMixIn(UserCredential.class, UserCredentialMixIn.class);
        mapper.addMixIn(AttributeBinding.class, AttributeBindingMixIn.class);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(module);
        return mapper;
    }
}
