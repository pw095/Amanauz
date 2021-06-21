import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryParser {
    public String fileName;
    private static final int START_LINE = 3;
    public ArrayList<String> businessFields;
    public ArrayList<String> businessFieldsInKey;
    public ArrayList<String> techFields;
    public QueryParser(String fileName){
        this.fileName = fileName;
    }
    public static void main(String[] args) {
    }
    public void printFields() {
        System.out.println("tech fields:");
        techFields.stream().forEach(System.out::println);
        System.out.println("business fields:");
        businessFields.stream().forEach(System.out::println);
    }
    public void parseQuery(){
        Path sourceFile = Paths.get(fileName);
        List<String> lines = null;
        try {
            lines = Files.readAllLines(sourceFile);
        } catch (IOException e) {
            e.getMessage();
        }
        fillFields(lines);
    }
    public ArrayList<String> getFields(String nameOfFiledsCollection) {
        ArrayList<String> result = new ArrayList<>();
        if (nameOfFiledsCollection == "businessFields") {
            result = this.businessFields;
        } else if (nameOfFiledsCollection == "businessFieldsInKey") {
            result = this.businessFieldsInKey;
        } else if (nameOfFiledsCollection == "techFields") {
            result = this.techFields;
        }
        return result;
    }
    private void fillFields(List<String> lines){
        this.businessFields         = new ArrayList<>();
        this.businessFieldsInKey    = new ArrayList<>();
        this.techFields = new ArrayList<>();
        boolean allLinesRead = false;

        for (int i = START_LINE; i < lines.size(); i++){
            allLinesRead = parseLine(lines.get(i));
            if (allLinesRead)
                break;
        }
    }

    private boolean parseLine(String line){
        String firstWord = line.trim().split("\\p{Space}")[0];
        if (firstWord.equalsIgnoreCase("primary")) {
//            techFields = new ArrayList<String>(Arrays.asList(
//                    (Arrays.stream(line.substring(line.indexOf('(') + 1, line.indexOf(')'))
//                            .split(", ")).filter(s->(!s.startsWith("tech$")))
//            ));
            Iterator<String> si =  Arrays.stream(line.substring(line.indexOf('(') + 1, line.indexOf(')'))
                    .split(", ")).filter(s->(!s.startsWith("tech$"))).iterator();
            while (si.hasNext())
                businessFieldsInKey.add(si.next());
            return true;
        }
        if (firstWord.startsWith("tech$"))
            techFields.add(firstWord);
        else
            businessFields.add(firstWord);
        return false;
    }
}
