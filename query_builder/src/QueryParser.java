import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class QueryParser {
    public String fileName;
    private static final int START_LINE = 3;
    public ArrayList<String> businessFields;
    public ArrayList<String> businessKeyFields;
    public ArrayList<String> businessNonKeyFields;
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
            fillFields(lines);
        } catch (IOException e) {
            e.getMessage();
        }

    }
    public ArrayList<String> getFields(String nameOfFieldsCollection) {
        ArrayList<String> result = new ArrayList<>();
        if (nameOfFieldsCollection.equals("businessFields")) {
            result = this.businessFields;
        } else if (nameOfFieldsCollection.equals("businessKeyFields")) {
            result = this.businessKeyFields;
        } else if (nameOfFieldsCollection.equals("businessNonKeyFields")) {
            result = this.businessNonKeyFields;
        } else if (nameOfFieldsCollection.equals("techFields")) {
            result = this.techFields;
        }
        return result;
    }
    private void fillFields(List<String> lines){
        this.businessFields         = new ArrayList<>();
        this.businessKeyFields      = new ArrayList<>();
        this.businessNonKeyFields   = new ArrayList<>();
        this.techFields             = new ArrayList<>();
        boolean allLinesRead = false;

        for (int i = START_LINE; i < lines.size(); i++){
            allLinesRead = parseLine(lines.get(i));
            if (allLinesRead)
                break;
        }
        businessFields.stream()
            .filter(x->!businessKeyFields.contains(x))
            .forEach(businessNonKeyFields::add);
//        for (String _field : this.businessFields) {
//            if (!this.businessKeyFields.contains(_field)){
//                this.businessNonKeyFields.add(_field);
//            }
//        }
    }

    private boolean parseLine(String line){
        String firstWord = line.trim().split("\\p{Space}")[0];
        if (firstWord.equalsIgnoreCase("primary")) {
            Arrays.stream(line.substring(line.indexOf('(') + 1, line.indexOf(')'))
                    .split(", ")).filter(s->(!s.startsWith("tech$")))
                    .forEach(x->businessKeyFields.add(x));
            return true;
        }
        if (firstWord.startsWith("tech$"))
            techFields.add(firstWord);
        else
            businessFields.add(firstWord);
        return false;
    }
}
