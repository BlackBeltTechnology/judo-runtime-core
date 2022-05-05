package hu.blackbelt.judo.services.dao.custom;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Builder
@EqualsAndHashCode
public class Gps {

    @NonNull
    private final double latitude;

    @NonNull
    private final double longitude;

    @Override
    public String toString() {
        return latitude + "," + longitude;
    }

    public static Gps parseString(final String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        } else {
            final String[] parts = value.split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid GPS: " + value);
            } else {
                return Gps.builder().latitude(Double.parseDouble(parts[0])).longitude(Double.parseDouble(parts[1])).build();
            }
        }
    }
}
