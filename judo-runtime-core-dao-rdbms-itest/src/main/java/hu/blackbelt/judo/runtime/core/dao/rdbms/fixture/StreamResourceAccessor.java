package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;

import liquibase.resource.AbstractResourceAccessor;
import liquibase.resource.InputStreamList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;


@RequiredArgsConstructor
public class StreamResourceAccessor extends AbstractResourceAccessor {

    @NonNull
    private final Map<String, InputStream> streams;

    @Override
    public InputStreamList openStreams(String relativeTo, String streamPath) throws IOException {
        InputStreamList returnList = new InputStreamList();

        streams.entrySet().stream()
                .filter(s -> escape(s.getKey())
                        .equals(escape(streamPath)))
                .map(s -> s.getValue()).forEach(s -> {
                    returnList.add(URI.create(escape(streamPath)), s);
                });
        return returnList;
    }

    @Override
    public SortedSet<String> list(String relativeTo, String path, boolean recursive, boolean includeFiles, boolean includeDirectories) {
        return describeLocations();
    }

    @Override
    public SortedSet<String> describeLocations() {
        SortedSet<String> returnSet = new TreeSet<>();
        streams.keySet().forEach(k ->
                returnSet.add(escape(k))
        );
        return returnSet;
    }

    String escape(String str) {
        return str.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}\\-\\_\\.]", "_");
    }
}
