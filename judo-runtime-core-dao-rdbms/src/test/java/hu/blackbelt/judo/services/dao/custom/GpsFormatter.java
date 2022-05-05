package hu.blackbelt.judo.services.dao.custom;

import hu.blackbelt.mapper.api.Formatter;

public class GpsFormatter implements Formatter<Gps> {

    @Override
    public String convertValueToString(final Gps value) {
        return value != null ? value.toString() : null;
    }

    @Override
    public Gps parseString(final String str) {
        return Gps.parseString(str);
    }

    @Override
    public Class<Gps> getType() {
        return Gps.class;
    }
}
