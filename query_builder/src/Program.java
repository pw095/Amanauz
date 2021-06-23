
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;


public class Program {
    private static String[] FieldsType;
    public static void main(String[] args) {
//     "..\\db\script\etl\repl"
//        if (args.length != 3) {
//            throw new RuntimeException("Неверное количество аргументов командной строки!");
//        }
//        String CatalogName = args[0];
        String CatalogName = "..\\db\\script\\etl\\repl";
        Path parentCatalog = Paths.get(CatalogName);
//        System.out.println("ParentCatName:" + CatalogName);
        Stream<Path> stream = null;
        try {
            stream = Files.walk(parentCatalog, 1);
        } catch (IOException e) {
            e.printStackTrace();
        };
        Iterator<Path> iterator = stream.filter(x->(!x.equals(parentCatalog) && !x.endsWith("data"))).iterator();

        QueryComposer qc = new QueryComposer("src");

        FieldsType = new String[3];
        FieldsType[0] = "businessFields";
        FieldsType[1] = "businessKeyFields";
        FieldsType[2] = "businessNonKeyFields";
//        FieldsType[2] = "techFields";

        while (iterator.hasNext()) {
            processEntity(iterator.next(), qc);

        }

    }

    private static void processEntity(Path EntityCatalog, QueryComposer qc){
        String entityName    = EntityCatalog.getName(EntityCatalog.getNameCount()-1).toString();
        String sourceFileName   = String.format("%s\\model\\%s.sql", EntityCatalog, entityName);
        String destFileName     = String.format("%s\\sql\\%s.sql", EntityCatalog, entityName);
        String destFileNameTech = String.format("%s\\sql\\tech$%s.sql", EntityCatalog, entityName);
        Map<String, String> map = new HashMap<String, String>();
        map.put("Pattern", destFileName);
        map.put("PatternTech", destFileNameTech);
        QueryParser qp = new QueryParser(sourceFileName);
        qp.parseQuery();
        System.out.println(entityName);
        for (Map.Entry<String, String> element : map.entrySet()) {
            qc.readPattern(element.getKey());
            qc.queryString.replace("%entity_name%",entityName);
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
//            System.out.println(qc.queryString);
//            try (BufferedWriter writer = Files.newBufferedWriter(
//                    Paths.get(destFileName), Charset.forName("UTF-8"))) {
//                writer.write(qc.queryString);
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException(e);
//            }
        }
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
                if (anc.isCondition) {
                    sb.append(offSet.substring(4));
                    sb.append("AND ");
                } else
                    sb.append(offSet);
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