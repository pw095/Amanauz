public class Program {
    public static void main(String[] args) {
        //
        String ddlQueryFileName = "";
        ddlQueryFileName = "D:\\0019  Курсы\\006  Java\\Amanauz\\src\\SourceQuery.sql";
//        String queryPattern = "D:\\0019  Курсы\\006  Java\\Amanauz\\src\\simple_pattern.sql";
        String queryPattern = "D:\\0019  Курсы\\006  Java\\Amanauz\\src\\MoreComplexPattern.sql";
        QueryParser qp = new QueryParser(ddlQueryFileName);
        qp.parseQuery();

        QueryComposer qc = new QueryComposer(queryPattern);
        qc.parsePatternFile();

        String[] FieldsType = new String[2];
        FieldsType[0] = "businessFields";
        FieldsType[1] = "businessFieldsInKey";
//        FieldsType[2] = "techFields";
        for (String fieldType : FieldsType) {
            int endInd = 0;
            while (true) {
                endInd = qc.queryString.indexOf(String.format("%%%s%%", fieldType), endInd + 1);
                if (endInd < 0) break;

                String currentFieldsOffSet = calcOffset(qc, endInd);
                modifyQuery(qc, qp, currentFieldsOffSet, fieldType);
            }
        }

        System.out.println(qc.queryString);

    }

    private static void modifyQuery(QueryComposer qc, QueryParser qp, String offSet, String fieldType) {
        StringBuilder sb2 = new StringBuilder();
        int numOfCurrentField = 1;
        for (String field: qp.getFields(fieldType)){
            if (numOfCurrentField > 1) {
                sb2.append(offSet);
            }
            sb2.append(field);
            if (numOfCurrentField < qp.getFields(fieldType).size()) {
                sb2.append(",\n");
//                sb2.append('\n');
            }
            numOfCurrentField++;
        }

        qc.queryString = qc.queryString.replaceFirst(
            String.format("%%%s%%", fieldType), sb2.toString());
    }

    private static String calcOffset(QueryComposer qc, int endInd) {

        int startInd = endInd - 1;

        for (; (qc.queryString.charAt(startInd) !='\n') && startInd > 0; startInd--);
        return (" ").repeat(endInd - startInd - 1);
    }
}