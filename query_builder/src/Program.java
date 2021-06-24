
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Program {
    private static String[] FieldsType;
    public static void main(String[] args) {

        if (args.length != 2) {
            throw new RuntimeException("Неверное количество аргументов командной строки!");
        }
        String CatalogName = args[0]; // "..\\db\\script\\etl\\repl";
//        String CatalogName = "..\\db\\script\\etl\\repl";
        String pathToPatterns = args[1]; // "src";
//        String pathToPatterns = "src";
        Path parentCatalog = Paths.get(CatalogName);

        QueryComposer qc = new QueryComposer(pathToPatterns);
        FieldsType = new String[3];
        FieldsType[0] = "businessFields";
        FieldsType[1] = "businessKeyFields";
        FieldsType[2] = "businessNonKeyFields";

        try {
            Files.walk(parentCatalog, 1)
                .filter(x->(!x.equals(parentCatalog) && !x.endsWith("data")))
                .forEach(x->processEntity(x, qc));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void processEntity(Path EntityCatalog, QueryComposer qc){
        String entityName    = EntityCatalog.getName(EntityCatalog.getNameCount()-1).toString();
        String sourceFileName   = String.format("%s\\model\\%s.sql", EntityCatalog, entityName);
        String destFileName     = String.format("%s\\sql\\%s.sql", EntityCatalog, entityName);
        String destFileNameTech = String.format("%s\\sql\\tech$%s.sql", EntityCatalog, entityName);
        Map<String, String> map = new HashMap<>();
        map.put("Pattern", destFileName);
        map.put("PatternTech", destFileNameTech);
        QueryParser qp = new QueryParser(sourceFileName);
        qp.parseQuery();
        for (Map.Entry<String, String> element : map.entrySet()) {
            qc.readPattern(element.getKey());
            qc.queryString = qc.queryString.replace("%entity_name%", entityName);
            for (String fieldType : FieldsType) {
                int begInd = 0;
                while (true) {
                    begInd = qc.queryString.indexOf(String.format("%%%s", fieldType), begInd + 1);
                    if (begInd < 0) break;
                    int endInd = qc.queryString.indexOf('%', begInd + 1);
                    String anchor = qc.queryString.substring(begInd + 1, endInd);

                    String currentFieldsOffSet = calcOffset(qc, begInd);
                    modifyQuery(qc, qp, currentFieldsOffSet, anchor);
                }
            }

            qc.queryString = qc.queryString + System.getProperty("line.separator");

            try (BufferedWriter writer = Files.newBufferedWriter(
                    Paths.get(element.getValue()), StandardCharsets.UTF_8)) { // Charset.forName("UTF-8")
                writer.write(qc.queryString);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static void modifyQuery(QueryComposer qc, QueryParser qp, String offSet, String anchor) {

        String fields = (anchor.equals("businessNonKeyFields")) ?
                ComposeConcatValueField(qp, offSet) : ComposeMultiFields(qp, offSet, anchor);

        qc.queryString = qc.queryString.replaceFirst(String.format("%%%s%%", anchor), fields);
    }

    private static String ComposeMultiFields(QueryParser qp, String offSet, String anchor){
//      anchor - строка типа businessKeyFields_condition_src_sat
        Anchor anc = new Anchor(anchor);

        StringBuilder sb = new StringBuilder();
        int numOfCurrentField = 1;
        for (String field: qp.getFields(anc.shortName )){
            if (numOfCurrentField > 1 && anc.multiLine) {
                if (anc.isCondition) {
                    sb.append(offSet.substring(4));
                    sb.append("AND ");
                } else
                    sb.append(offSet);
            }
            if (anc.firstRel.isEmpty())
                sb.append(field);
            else if (!anc.secondRel.isEmpty())
                sb.append(String.format("%s.%s = %s.%s", anc.firstRel, field, anc.secondRel, field));
            else
                sb.append(String.format("%s.%s", anc.firstRel, field));
            if (numOfCurrentField < qp.getFields(anc.shortName).size()) {
                if (anc.multiLine)
                    sb.append(anc.secondRel.isEmpty() ? ",": "").append(System.getProperty("line.separator"));
                else
                    sb.append(", ");
            }
            numOfCurrentField++;
        }
        return sb.toString();
    }

    private static String ComposeConcatValueField(QueryParser qp, String offSet) {

        StringBuilder sb = new StringBuilder();
        int numOfCurrentField = 1;
        ArrayList<String> fields = qp.getFields("businessNonKeyFields");
        int stdLength = fields.stream().max(Comparator.comparingInt(String::length)).get().length();
        for (String field : fields){
            if (numOfCurrentField > 1) {
                sb.append(offSet);
            }
            sb.append(String.format("'_' || IFNULL(CAST(%-"+stdLength+"s AS TEXT), '%s') ||", field, "!@#\\$%^&*"));
            if (numOfCurrentField < fields.size()) {
                sb.append(System.getProperty("line.separator"));
            }
            numOfCurrentField++;
        }
        sb.append(" '_'");
        return sb.toString();
    }

    private static String calcOffset(QueryComposer qc, int endInd) {

        int startInd = endInd - 1;
        StringBuilder sb = new StringBuilder();
        for (; (qc.queryString.charAt(startInd) !='\n') && startInd > 0; startInd--)
            sb.append(" ");
        return sb.toString();
    }
}