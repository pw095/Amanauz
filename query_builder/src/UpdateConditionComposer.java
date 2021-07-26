import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class UpdateConditionComposer {
    private static String templateLine;

    public static void setPathToPatterns(String pathToPatterns){

        Path sourceFile = Paths.get(pathToPatterns+"\\UpdateConditionPattern.sql");
        try {
            templateLine = Files.readAllLines(sourceFile).get(0);
        } catch (IOException e) {
            e.getMessage();
        }
    }

    public String getCondition(String fieldName) {
        return getCondition(fieldName, "");
    }

    public String getCondition(String fieldName, String prefix) {

        StringBuilder sb = new StringBuilder();

        sb.append(String.format("%s%s", prefix, templateLine.replace("@field_name@", fieldName)));

        return sb.toString();

    }
}
