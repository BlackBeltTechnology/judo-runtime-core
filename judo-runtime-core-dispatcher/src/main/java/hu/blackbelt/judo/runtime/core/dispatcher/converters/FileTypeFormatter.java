package hu.blackbelt.judo.runtime.core.dispatcher.converters;

import com.google.gson.Gson;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.mapper.api.Formatter;

import java.util.Map;
import java.util.TreeMap;

public class FileTypeFormatter implements Formatter<FileType> {

    private static final String KEY_ID = "id";
    private static final String KEY_FILENAME = "fileName";
    private static final String KEY_SIZE = "size";
    private static final String KEY_MIME_TYPE = "mimeType";

    @Override
    public String convertValueToString(final FileType fileType) {
        final Map<String, Object> map = new TreeMap<>();
        map.put(KEY_ID, fileType.getId());
        map.put(KEY_FILENAME, fileType.getFileName());
        map.put(KEY_SIZE, fileType.getSize());
        map.put(KEY_MIME_TYPE, fileType.getMimeType());
        return new Gson().toJson(map);
    }

    @Override
    public FileType parseString(final String str) {
        @SuppressWarnings("unchecked")
		final Map<String, Object> map = new Gson().fromJson(str, Map.class);
        return FileType.builder()
                .id((String) map.get(KEY_ID))
                .fileName((String) map.get(KEY_FILENAME))
                .size(map.get(KEY_SIZE) != null ? Double.valueOf(map.get(KEY_SIZE).toString()).longValue() : null)
                .mimeType((String) map.get(KEY_MIME_TYPE))
                .build();
    }

    @Override
    public Class<FileType> getType() {
        return FileType.class;
    }
}
