import java.sql.Struct;
import java.util.ArrayList;
import java.util.Map;

public class Program {
    public static void main(String[] args) {
        // взять отсюда:
        // 
        String ddlQueryFileName = "";
        ddlQueryFileName = "D:\\0021  GitHub\\Amanauz\\query_builder\\src\\SourceQuery.sql";
//        String queryPattern = "D:\\0019  Курсы\\006  Java\\Amanauz\\src\\simple_pattern.sql";
        String queryPattern = "D:\\0021  GitHub\\Amanauz\\query_builder\\src\\Pattern.sql";
        QueryParser qp = new QueryParser(ddlQueryFileName);
        qp.parseQuery();

        QueryComposer qc = new QueryComposer(queryPattern);
        qc.parsePatternFile();

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

        for (; (qc.queryString.charAt(startInd) !='\n') && startInd > 0; startInd--);
        return (" ").repeat(endInd - startInd - 1);
    }
}