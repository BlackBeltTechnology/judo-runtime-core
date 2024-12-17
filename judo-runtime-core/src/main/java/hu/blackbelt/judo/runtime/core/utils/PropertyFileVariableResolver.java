package hu.blackbelt.judo.runtime.core.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Builder
@Slf4j
public class PropertyFileVariableResolver implements VariableResolver {
    public static final String PROPERTIES_FILES = "propertiesFiles";
    @NonNull
    final File parameterDirectory;
    @NonNull
    final Collection<File> parameterFiles;


    @Override
    public String getName() {
        return PROPERTIES_FILES;
    }

    @Override
    public Map<String, Object> process() {
        Map<String, Object> variables = new LinkedHashMap<>();
        if (parameterFiles != null && parameterFiles.size() > 0) {
            for (File parameterFile : parameterFiles) {
                if (parameterFile != null) {
                    if (!parameterFile.exists() && parameterDirectory != null) {
                        parameterFile = new File(parameterDirectory, parameterFile.getName());
                    }
                    if (parameterFile.exists()) {
                        Properties prop = new Properties();
                        try (Reader reader = Files.newBufferedReader(parameterFile.toPath(), StandardCharsets.UTF_8)){
                            prop.load(reader);
                            variables.putAll(VariableResolver.generalizeTemplateVariableNames(
                                    Maps.fromProperties(prop).entrySet().stream().collect(Collectors.toMap(
                                            e -> String.valueOf(e.getKey()),
                                            e -> String.valueOf(e.getValue()),
                                            (prev, next) -> next, HashMap::new
                                    ))));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        log.warn("File is missing:" + parameterFile.getAbsolutePath());
                    }
                }
            }
        }
        return variables;
    }
}
