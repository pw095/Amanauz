import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class QueryComposer {
    String PatternsCatalog;
    String queryString;

    public QueryComposer(String PatternsCatalog){
        this.PatternsCatalog = PatternsCatalog;
    }
    public void readPattern(String pattern){
        Path path = Paths.get(String.format("%s\\%s.sql", PatternsCatalog, pattern));
        try {
            queryString = Files.lines(path).collect(Collectors.joining(System.getProperty("line.separator")));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
