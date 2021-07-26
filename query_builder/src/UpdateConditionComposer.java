import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class UpdateConditionComposer {
    private ArrayList<String> TemplateLines;

    public UpdateConditionComposer(){
       this("src\\UpdateConditionPattern.sql");
    }
    public UpdateConditionComposer(String fileName){
        Path sourceFile = Paths.get(fileName);
        try {
            TemplateLines = (ArrayList<String>) Files.readAllLines(sourceFile);
        } catch (IOException e) {
            e.getMessage();
        }
    }

    public String getCondition(String fieldName) {
        return getCondition(fieldName, "");
    }

    public String getCondition(String fieldName, String prefix) {

        StringBuilder sb = new StringBuilder();
        for (String line : TemplateLines) {
            sb.append(String.format("%s%s\n", prefix, line.replace("@field_name@", fieldName)));
        }
        return sb.toString();

    }
}
