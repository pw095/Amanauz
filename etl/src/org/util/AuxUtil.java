package org.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collectors;

public class AuxUtil {

    public static final DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String getQuery(String pathString) {
        String queryString = null;
        Path path = Paths.get(pathString);
        try {
            queryString = Files.lines(path).collect(Collectors.joining(System.getProperty("line.separator")));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return queryString;
    }

    public static Map<String, String> getProperties(String pathString) {
        Map<String, String> propertiesMap = null;
        Path path = Paths.get(pathString);
        try {
            propertiesMap = Files.lines(path)
                                .filter(str -> !str.startsWith("#"))
                                .filter(str -> !str.isEmpty())
                                .collect(Collectors.toMap(str -> str.split("=")[0], str -> str.split("=")[1]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return propertiesMap;
    }
}
