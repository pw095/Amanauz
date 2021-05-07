import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {

    private static int pageSize;
    private static int pageCount;

    private static String previousDate = null;
    private static String currentDate = null;

    private static List<IndexSecurityData> indexArrayData = new ArrayList<>();

    static class Tuple {
        private String secid;
        private Double value;
        public Tuple(JSONArray list) {
            secid = list.optString(0);
            value = list.optDouble(1);
        }
    }

    private static void getIndexMetaInfo() throws Exception {

        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics.cursor&analytics.cursor.columns=TOTAL,PAGESIZE,PREV_DATE";
        URL url = new URL(urlString.concat(currentDate == null ? "" : "&date=" + currentDate));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        int status = conn.getResponseCode();
        try(BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String inputLine;
            StringBuffer content = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            Iterator<Object> iter = new JSONObject(content.toString())
                    .optJSONObject("analytics.cursor")
                    .getJSONArray("data").iterator();

            while (iter.hasNext()) {
                JSONArray test = (JSONArray) iter.next();
                pageSize = test.optInt(1);
                pageCount = (int) Math.ceil(test.optDouble(0)/test.optDouble(1));
                previousDate = test.optString(2);
            }
        }
        conn.disconnect();
    }

    private static void getData() throws Exception {

        String urlString = "http://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/IMOEX.json?iss.meta=off&iss.only=analytics&analytics.columns=indexid,tradedate,secids,weight";
        String indexName = "IMOEX";

        for(int ind=0; ind < pageCount; ind++) {
            URL url = new URL(urlString.concat(currentDate == null ? "" : "&date=" + currentDate).concat("&start=" + String.valueOf(ind*pageSize)));
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            try(BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                Map<String, Double> inMap = new HashMap<>();
                String inputLine;
                StringBuffer content = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }

                Iterator<Object> iter = new JSONObject(content.toString())
                                            .optJSONObject("analytics")
                                            .getJSONArray("data").iterator();

                while (iter.hasNext()) {
                    JSONArray test = (JSONArray) iter.next();
                    indexArrayData.add(new IndexSecurityData(test.optString(0), test.optString(1), test.optString(2), test.optDouble(3)));
                }
            }
            conn.disconnect();
        }
    }

    public static void main(String[] args) throws Exception {
/*
        List<Element<String>> arr = new ArrayList<>();
        arr.add(new Element<String>("index_hub"));
        arr.add(new Element<String>("security_hub"));
        arr.add(new Element<String>("index_security_link", "index_hub", "security_hub"));
        arr.add(new Element<String>("index_sat", "index_security_link", "index_hub"));
        HTree<Element<String>, String> hTree = new HTree<>(arr);
*/
        List<Element<Entity>> arr = Collections.synchronizedList(new ArrayList<>());
        Map<String, Entity> map = new HashMap<>();
/*

//        map.put("StgIndexSecurityWeight", new StgIndexSecurityWeight());
        map.put("OdsIndexSecurityWeight", new OdsIndexSecurityWeight());
        map.put("DdsIndexHub", new DdsIndexHub());
        map.put("DdsIndexSat", new DdsIndexSat());

//        arr.add(new Element<Entity>(map.get("StgIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("OdsIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("DdsIndexHub"), map.get("OdsIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("DdsIndexSat"), map.get("OdsIndexSecurityWeight"), map.get("DdsIndexHub")));
*/

        map.put("StgIndexSecurityWeight", new StgIndexSecurityWeight());
        map.put("OdsIndexSecurityWeight", new OdsIndexSecurityWeight());
        map.put("DdsIndexHub", new DdsIndexHub());
        map.put("DdsIndexSat", new DdsIndexSat());

        map.put("StgSecurityRate", new StgSecurityRate());
        map.put("OdsSecurityRate", new OdsSecurityRate());
        map.put("DdsSecurityHub", new DdsSecurityHub());
        map.put("DdsSecuritySat", new DdsSecuritySat());

        map.put("DdsIndexSecurityLink", new DdsIndexSecurityLink());
        map.put("DdsIndexSecuritySat", new DdsIndexSecuritySat());



        arr.add(new Element<Entity>(map.get("StgIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("OdsIndexSecurityWeight"), map.get("StgIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("DdsIndexHub"), map.get("OdsIndexSecurityWeight")));
        arr.add(new Element<Entity>(map.get("DdsIndexSat"), map.get("OdsIndexSecurityWeight"), map.get("DdsIndexHub")));

        arr.add(new Element<Entity>(map.get("StgSecurityRate")));
        arr.add(new Element<Entity>(map.get("OdsSecurityRate"), map.get("StgSecurityRate")));
        arr.add(new Element<Entity>(map.get("DdsSecurityHub"), map.get("OdsSecurityRate")));
        arr.add(new Element<Entity>(map.get("DdsSecuritySat"), map.get("OdsSecurityRate"), map.get("DdsSecurityHub")));

        arr.add(new Element<Entity>(map.get("DdsIndexSecurityLink"), map.get("DdsIndexHub"), map.get("DdsSecurityHub")));
        arr.add(new Element<Entity>(map.get("DdsIndexSecuritySat"), map.get("DdsIndexSecurityLink"), map.get("OdsIndexSecurityWeight")));

        HTree<Element<Entity>, Entity> hTree = new HTree<>(arr);
        ExecutorService service = null;

        try {
//            service = Executors.newCachedThreadPool();

//            List<Future<Entity>> listEntityFuture = Collections.synchronizedList(new ArrayList<>());
            Set<Future<Entity>> listEntityFuture = new HashSet<>();

//            List<Future<Entity>> listEntityFuture = new CopyOnWriteArrayList<>();
//            Set<Future<Entity>> listEntityFuture = new ConcurrentSkipListSet<>();
            service = Executors.newFixedThreadPool(3);

            for (Element<Entity> element: hTree.getRoots()) {
                listEntityFuture.add(service.submit(element));
            }

            while(true) {
//                System.out.println("--------------------------");
//                List<Element<Entity>> elementList = new ArrayList<>();
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Set<Element<Entity>> elementList = new HashSet<>();

                System.out.println("listEntityFuture.size=" + listEntityFuture.size());
                List<Entity> finishedList = listEntityFuture
                                            .stream()
                                            .filter(Future::isDone)
                                            .map(t -> {Entity ent = null;
                                                        try {
                                                            ent = t.get();
                                                        } catch (Exception e) {}
                                                        return ent;
                                                      })
                                            .collect(Collectors.toList());
                for (Entity finElement : finishedList) {
                    elementList.addAll(hTree.getChildren(hTree.getById(finElement))
                                            .stream()
                                            .filter(t -> t.getId().getStatus() == LoadStatus.ENQUEUED)
                                            .filter(t -> t.getParentIds().allMatch(entity -> entity.getStatus() == LoadStatus.SUCCEEDED))
                                            .collect(Collectors.toSet()));

                }
                System.out.println("elementList.size=" + elementList.size() + " elementList: " + elementList.stream().map(Element::getId).map(t -> t.entityName).collect(Collectors.joining(", ")));
/*
                for (Future<Entity> entityFuture : listEntityFuture) {

//                    System.out.println("listEntityFuture.size=" + listEntityFuture.size());

                    try {
                         elementList.addAll(hTree.getChildren(hTree.getById(entityFuture.get(1000, TimeUnit.MILLISECONDS)))
                                 .stream()
                                 .filter(t -> t.getId().getStatus() == LoadStatus.ENQUEUED)
                                 .filter(t -> !t.getParentIds().filter(entity -> entity.getStatus() != LoadStatus.SUCCEEDED).findFirst().isPresent())
                                 .collect(Collectors.toSet()));
                        } catch (TimeoutException e) {
                        }
//                    System.out.println("elementList.size=" + elementList.size() + " elementList: " + elementList.stream().map(Element::getId).map(t -> t.entityName).collect(Collectors.joining(", ")));
                }
*/

                if (hTree.getLeaves().stream().allMatch(element -> element.getId().getStatus() == LoadStatus.SUCCEEDED) && listEntityFuture.stream().allMatch(Future::isDone)) {
                    break;
                }
                for(Element<Entity> element : elementList) {
                    listEntityFuture.add(service.submit(element));
//                    System.out.println("addResult: " + );
                }
            }

//            service.invokeAll(arr);
/*        } catch (InterruptedException e) {
            e.printStackTrace();*/
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }
        System.out.println("FINISHED");
//        hTree.getById("index_hub");

//        hTree.getParents(hTree.getById("index_hub")).stream().map(Element::getId).forEach(System.out::println);
//        hTree.getChildren(hTree.getById("index_hub")).stream().map(Element::getId).forEach(System.out::println);

//        hTree.getRoots().stream().map(Element::getId).forEach(System.out::println);
//        System.out.println(hTree.isLeaf(hTree.getById("index_security_link")));

/*
        arr.stream().filter(element -> element.checkIfEquals("index_hub")).findFirst().map(Element::getId).ifPresent(System.out::println);
        arr.stream()
                .filter(element -> element.checkIfEquals("index_hub"))
                .findFirst().orElseThrow(RuntimeException::new).getParentIds();
                */


/*        while(currentDate == null || !LocalDate.parse(currentDate, DateTimeFormatter.ISO_LOCAL_DATE).isBefore(LocalDate.of(2021,03,26))) {
            getIndexMetaInfo();
            getData();
            currentDate = previousDate;
        }

        String sqlString = "INSERT INTO index_security_data(index_name, trade_date, secid, weight, tech$load_date) VALUES(?, ?, ?, ?, ?)";
        String techLoadDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        try(Connection conn = DriverManager.getConnection("jdbc:sqlite:C:\\Users\\pw095\\IdeaProjects\\JSON_APP\\stage.db");
                PreparedStatement stmt = conn.prepareStatement(sqlString)) {

            for(IndexSecurityData iter : indexArrayData) {
                stmt.setString(1, iter.getIndexName());
                stmt.setString(2, iter.getTradeDate());
                stmt.setString(3, iter.getSecurityId());
                stmt.setDouble(4, iter.getWeight());
                stmt.setString(5, techLoadDate);
                stmt.addBatch();
            }
            stmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }*/

/*        List<Entity> arr = new ArrayList<>();
        arr.add(new IndexSecurity());
        arr.add(new SecurityRates());

        for(Entity ind : arr) {
            ind.entityLoad();
        }*/
    }
}
