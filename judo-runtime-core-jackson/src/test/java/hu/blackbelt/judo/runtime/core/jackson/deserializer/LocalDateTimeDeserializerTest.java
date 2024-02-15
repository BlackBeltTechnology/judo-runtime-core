package hu.blackbelt.judo.runtime.core.jackson.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.time.format.DateTimeParseException;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LocalDateTimeDeserializerTest {

    @Test
    public void testLocalDateTimeDeserialization() {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule()
                .addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());

        objectMapper.registerModule(simpleModule);

        String ldtRaw = "2023-04-25T11:11:11.111";
        assertThat(objectMapper.convertValue(ldtRaw, LocalDateTime.class), equalTo(LocalDateTime.parse(ldtRaw, ISO_LOCAL_DATE_TIME)));

        String ldt1Raw = "2023-04-25T13:11:11.111+02:00";
        assertThat(objectMapper.convertValue(ldt1Raw, LocalDateTime.class),
                   equalTo(OffsetDateTime.parse(ldt1Raw, ISO_OFFSET_DATE_TIME).atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));

        assertThrows(DateTimeParseException.class, () -> objectMapper.convertValue("15", LocalDateTime.class));
    }

}
