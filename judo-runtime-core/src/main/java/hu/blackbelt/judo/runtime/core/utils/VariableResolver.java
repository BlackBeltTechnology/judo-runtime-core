package hu.blackbelt.judo.runtime.core.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public interface VariableResolver {
    String getName();
    Map<String, Object> process();

    static Map<String, Object> generalizeTemplateVariableNames(Map<String, Object> parameters) {
        return parameters.entrySet().stream().filter(e -> e.getKey() != null && e.getValue() != null).collect(
                Collectors.toMap(
                        e -> generalizeName(String.valueOf(e.getKey())),
                        e -> e.getValue(),
                        (prev, next) -> next, HashMap::new
                ));
    }

    static String generalizeName(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        boolean isUpperSnakeCae = !str.matches(negateRegex("([A-Z_0-9]*)"));
        if (isUpperSnakeCae) {
            return toCamelCase(Arrays.stream(str.split("_")).map(s -> capitalize(s)).collect(Collectors.joining()));
        } else if (str.matches(".*[.#@_,;:-].*")) {
            return toCamelCase(Arrays.stream(str.split("[.#@_,;:-]")).map(s -> firstToUpper(s)).collect(Collectors.joining()));
        }
        return toCamelCase(str);
    }

    static String negateRegex(String regex) {
        return "(?!(?:" + regex + ")$).*";
    }

    static String toCamelCase(String str) {
        String[] words = str.split("(?=[A-Z])");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            if (i == 0) {
                word = word.isEmpty() ? word : word.toLowerCase();
            } else {
                word = word.isEmpty() ? word : capitalize(word);
            }
            builder.append(word);
        }
        return builder.toString();
    }
    static String firstToUpper(String str) {
        if (str != null && str.length() > 1) {
            return str.substring(0, 1).toUpperCase() + str.substring(1);
        }
        return str;
    }

    static String firstToLower(String str) {
        if (str != null && str.length() > 1) {
            return str.substring(0, 1).toLowerCase() + str.substring(1);
        }
        return str;
    }

    static String capitalize(String str) {
        if (str != null && str.length() > 1) {
            return str.substring(0,1).toUpperCase() + str.substring(1).toLowerCase();
        }
        return str;
    }
}
