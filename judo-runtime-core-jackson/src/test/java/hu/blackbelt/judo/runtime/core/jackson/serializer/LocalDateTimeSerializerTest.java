package hu.blackbelt.judo.runtime.core.jackson.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LocalDateTimeSerializerTest {

    @Test
    public void testLocalDateTimeSerialization() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule()
                .addSerializer(LocalDateTime.class, new LocalDateTimeSerializer());

        objectMapper.registerModule(simpleModule);

        LocalDateTime ldt = LocalDateTime.now();
        LocalDateTime ldt1 = LocalDateTime.parse("2023-04-24T17:00");
        Map<String, Object> testMap = Map.of(
                "ldt", ldt,
                "ldt1", ldt1
        );

        String ldtAsOffsetDateTimeString = ldt.atOffset(ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
        String ldt1AsOffsetDateTimeString = ldt1.atOffset(ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
        assertThat(objectMapper.writeValueAsString(testMap), CoreMatchers.anyOf(
                equalTo("{" +
                            "\"ldt\":\"" + ldtAsOffsetDateTimeString + "\"," +
                            "\"ldt1\":\"" + ldt1AsOffsetDateTimeString + "\"" +
                        "}"),
                equalTo("{" +
                            "\"ldt1\":\"" + ldt1AsOffsetDateTimeString + "\"," +
                            "\"ldt\":\"" + ldtAsOffsetDateTimeString + "\"" +
                        "}")
        ));
    }

}
