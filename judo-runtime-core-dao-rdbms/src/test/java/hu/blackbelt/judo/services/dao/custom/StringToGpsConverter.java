package hu.blackbelt.judo.services.dao.custom;

import hu.blackbelt.mapper.api.Converter;

public class StringToGpsConverter implements Converter<String, Gps> {

    @Override
    public Class<String> getSourceType() {
        return String.class;
    }

    @Override
    public Class<Gps> getTargetType() {
        return Gps.class;
    }

    @Override
    public Gps apply(final String s) {
        return Gps.parseString(s);
    }
}
