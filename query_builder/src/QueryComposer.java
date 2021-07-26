import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;

public class QueryComposer {

    private EntityParser entityParser;
    private String queryPattern;
    private String queryString;
    private String entityName;
    private static String[] FieldsType;

    static {
        FieldsType = new String[4];
        FieldsType[0] = "businessFields";
        FieldsType[1] = "businessKeyFields";
        FieldsType[2] = "businessNonKeyFields";
        FieldsType[3] = "updateCondition";
    }

    public QueryComposer(EntityParser entityParser, String entityName){
        this.entityParser = entityParser;
        this.entityName = entityName;
    }

    /*
     * Метод читает шаблон запроса из файлов
     * сейчас это либо Pattern.sql, либо PatternTech.sql
     * и возвращает содержимое в виде строки*/
    public void setPattern(String pathToPatterns, String patternName){

        setPattern(String.format("%s\\%s.sql", pathToPatterns, patternName));

    }
    public void setPattern(String fullPathToPattern){

        Path path = Paths.get(fullPathToPattern);
        try {
            queryPattern = Files.lines(path).collect(Collectors.joining(System.getProperty("line.separator")));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    public void processQuery() {

        queryString = queryPattern.replace("%entity_name%", entityName);

        for (String fieldType : FieldsType) {
            int begInd = 0;
            while (true) {
                begInd = queryString.indexOf(String.format("%%%s", fieldType), begInd + 1);
                if (begInd < 0) break;
                int endInd = queryString.indexOf('%', begInd + 1);
                String anchor = queryString.substring(begInd + 1, endInd);

                String currentFieldsOffSet = calcOffset(begInd);
                modifyQuery(currentFieldsOffSet, anchor);
            }
        }
        queryString = queryString + System.getProperty("line.separator");
    }

    private void modifyQuery(String offSet, String anchor) {

        String fields = "";
        if (anchor.equals("businessNonKeyFields"))
            fields = ComposeConcatValueField(offSet);
        else if (anchor.equals("updateCondition"))
//            fields = ComposeUpdateStatements(offSet);
            fields = ComposeUpdateStatements("");
        else
            fields = ComposeMultiFields(offSet, anchor);

        queryString = queryString.replaceFirst(String.format("%%%s%%", anchor), fields);
    }


    private String ComposeUpdateStatements(String offSet) {
        StringBuilder sb = new StringBuilder();


        for (String field: entityParser.getFields("businessNonKeyFields")){
            UpdateConditionComposer upd = new UpdateConditionComposer();
//            sb.append(String.format(",%s\n", upd.getCondition(field, offSet)));
            sb.append(upd.getCondition(field, offSet));
        }
        return sb.toString();
    }

    private String ComposeMultiFields(String offSet, String anchor){
//      anchor - строка типа businessKeyFields_condition_src_sat
        Anchor anc = new Anchor(anchor);

        StringBuilder sb = new StringBuilder();
        int numOfCurrentField = 1;
        for (String field: entityParser.getFields(anc.shortName )){
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
            if (numOfCurrentField < entityParser.getFields(anc.shortName).size()) {
                if (anc.multiLine)
                    sb.append(anc.secondRel.isEmpty() ? ",": "").append(System.getProperty("line.separator"));
                else
                    sb.append(", ");
            }
            numOfCurrentField++;
        }
        return sb.toString();
    }

    private String ComposeConcatValueField(String offSet) {

        StringBuilder sb = new StringBuilder();
        int numOfCurrentField = 1;
        ArrayList<String> fields = entityParser.getFields("businessNonKeyFields");
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

    private String calcOffset(int endInd) {

        int startInd = endInd - 1;
        StringBuilder sb = new StringBuilder();
        for (; (this.queryString.charAt(startInd) !='\n') && startInd > 0; startInd--)
            sb.append(" ");
        return sb.toString();
    }
    public String getQueryString(){
        return this.queryString;
    }
}

