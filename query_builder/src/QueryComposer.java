import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class QueryComposer {
    String patternFilename;
    String queryString;
    public QueryComposer(String patternFilename){
        this.patternFilename = patternFilename;
    }
    public void parsePatternFile(){
        Path path = Paths.get(patternFilename);
        try {
            queryString = Files.lines(path).collect(Collectors.joining(System.getProperty("line.separator")));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
