
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Program {
    private static String[] FieldsType;
    private static Map<String, String> patternVars;
    private static String pathToPatterns;
    public static void setPathToPatterns(String path){
        pathToPatterns = path;
    }
    public static void main(String[] args) {

        if (args.length != 2) {
            throw new RuntimeException("Неверное количество аргументов командной строки!");
        }
        String catalogName = args[0]; // "..\\db\\script\\etl\\repl";
//        String CatalogName = "..\\db\\script\\etl\\repl";
        pathToPatterns = args[1]; // "src";
//        String pathToPatterns = "src";
        processEntitiesInCatalog(catalogName);

    }

    public static void InitialisePatternVars(String destFileName, String destFileNameTech) {

        patternVars = new HashMap<>();
        if (!destFileName.isEmpty())
            patternVars.put("Pattern", destFileName);
        if (!destFileNameTech.isEmpty())
            patternVars.put("PatternTech", destFileNameTech);
    }

    /*
    * Метод обрабатывает сущности-таблицы во вложенных каталогах,
    * например "default_data_board", "default_data_currency"
    * catalogName - обычно это "Amanauz\db\script\etl\repl"
    * */
    public static void processEntitiesInCatalog(String catalogName){
        Path parentCatalog = Paths.get(catalogName);

        try {
            Files.walk(parentCatalog, 1)
                    .filter(x->(!x.equals(parentCatalog) && !x.endsWith("data")))
                    .forEach(x->processEntityInCatalog(x));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processEntityInCatalog(Path EntityCatalog){
        String entityName    = EntityCatalog.getName(EntityCatalog.getNameCount()-1).toString();
        String sourceFileName   = String.format("%s\\model\\%s.sql", EntityCatalog, entityName);
        String destFileName     = String.format("%s\\sql\\%s.sql", EntityCatalog, entityName);
        String destFileNameTech = String.format("%s\\sql\\tech$%s.sql", EntityCatalog, entityName);

        InitialisePatternVars(destFileName, destFileNameTech);
        processEntityInFile(sourceFileName, entityName);
    }

    public static void processEntityInFile(String sourceFileName, String entityName) {
        EntityParser entityParser = new EntityParser(sourceFileName);
        entityParser.parseEntity();

        for (Map.Entry<String, String> element : patternVars.entrySet()) {
            QueryComposer.setPathToPatterns(pathToPatterns);
            QueryComposer composer = new QueryComposer(entityParser, entityName);
            composer.setPattern(pathToPatterns, element.getKey());
            composer.processQuery();

            try (BufferedWriter writer = Files.newBufferedWriter(
                    Paths.get(element.getValue()), StandardCharsets.UTF_8)) { // Charset.forName("UTF-8")
                writer.write(composer.getQueryString());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

    }
}