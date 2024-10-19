package hu.blackbelt.judo.runtime.core.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import hu.blackbelt.judo.runtime.core.jackson.deserializer.LocalDateTimeDeserializer;
import hu.blackbelt.judo.runtime.core.jackson.serializer.LocalDateTimeSerializer;

import java.time.LocalDateTime;

public class ObjectMapperFactory {

    public static ObjectMapper createObjectMapper() {
        SimpleModule customSerializerModule = new SimpleModule()
                .addSerializer(LocalDateTime.class, new LocalDateTimeSerializer())
                .addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new JSR353Module())
                .registerModule(customSerializerModule);

        mapper.findAndRegisterModules();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }
}
