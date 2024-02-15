package hu.blackbelt.judo.runtime.core.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

    @Override
    public LocalDateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        String rawTimestamp = jsonParser.getText();

        try {
            return LocalDateTime.parse(rawTimestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(rawTimestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME).atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
        }
    }

}
