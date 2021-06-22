import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Program {
    public static void main(String[] args) {
        // взять отсюда:
        // 
//        String ddlQueryFileName = "";
        if (args.length != 3) {
            throw new RuntimeException("Неверное количество аргументов командной строки!");
        }
        String ddlQueryFileName = args[0];
        String queryPattern = args[1];
        String destQuery = args[2];
//        ddlQueryFileName = "D:\\0021  GitHub\\Amanauz\\query_builder\\src\\SourceQuery.sql";
//        String queryPattern = "D:\\0019  Курсы\\006  Java\\Amanauz\\src\\simple_pattern.sql";
//        String queryPattern = "D:\\0021  GitHub\\Amanauz\\query_builder\\src\\Pattern.sql";
        QueryParser qp = new QueryParser(ddlQueryFileName);
        qp.parseQuery();
//        System.out.println("0" + args[0]);
//        System.out.println("1" + args[1]);

        QueryComposer qc = new QueryComposer(queryPattern);
        qc.parsePatternFile();
//        System.out.println(qc.queryString);
        String[] FieldsType = new String[3];
        FieldsType[0] = "businessFields";
        FieldsType[1] = "businessKeyFields";
        FieldsType[2] = "businessNonKeyFields";
//        FieldsType[2] = "techFields";
        for (String fieldType : FieldsType) {
            int begInd = 0;
            while (true) {
                begInd = qc.queryString.indexOf(String.format("%%%s", fieldType), begInd + 1);
                if (begInd < 0) break;
                int endInd = qc.queryString.indexOf('%', begInd+1);
                String anchor = qc.queryString.substring(begInd+1, endInd);

                String currentFieldsOffSet = calcOffset(qc, begInd);
                modifyQuery(qc, qp, currentFieldsOffSet, anchor);
            }
        }

        Path path = Paths.get(destQuery);
        try (BufferedWriter writer = Files.newBufferedWriter(path, Charset.forName("UTF-8"))) {
            writer.write(qc.queryString);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println(qc.queryString);

    }

    private static void modifyQuery(QueryComposer qc, QueryParser qp, String offSet, String anchor) {

        String fields = "";
        if (anchor.equals("businessNonKeyFields")) {
            fields = ComposeConcatValueField(qp, offSet);
        } else {
            fields = ComposeMultiFields(qp, offSet, anchor);
        }
        qc.queryString = qc.queryString.replaceFirst(String.format("%%%s%%", anchor), fields);
    }

    private static String ComposeMultiFields(QueryParser qp, String offSet, String anchor){
//      anchor - строка типа businessKeyFields_condition_src_sat
        Anchor anc = new Anchor(anchor);

        StringBuilder sb = new StringBuilder();
        int numOfCurrentField = 1;
        for (String field: qp.getFields(anc.shortName )){
            if (numOfCurrentField > 1 && anc.multiLine) {
                sb.append(offSet);
                if (anc.isCondition)
                    sb.append("AND ");
            }
            if (anc.firstRel.isEmpty())
                sb.append(field);
            else if (!anc.firstRel.isEmpty() && !anc.secondRel.isEmpty())
                sb.append(String.format("%s.%s = %s.%s", anc.firstRel, field, anc.secondRel, field));
            else
                sb.append(String.format("%s.%s", anc.firstRel, field));
            if (numOfCurrentField < qp.getFields(anc.shortName).size()) {
                if (anc.multiLine)
                    sb.append(",\n");
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
        for (String field : fields){
            if (numOfCurrentField > 1) {
                sb.append(offSet);
            }
            sb.append(String.format("'_' || IFNULL(CAST(%s AS TEXT), '%s') ||", field, "!@#%^&*\\$"));
            if (numOfCurrentField < fields.size()) {
                sb.append('\n');
            }
            numOfCurrentField++;
        }
        sb.append('_');
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