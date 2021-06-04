package org.data;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.util.AuxUtil.dateFormat;

public interface ImoexSourceEntity extends ExternalSourceEntity {
    public abstract <T extends ExternalData> T accumulateData(JSONArray jsonArray);

    default public List<? extends ExternalData> load(String urlString, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        try {
            System.out.println(urlString);
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            conn.getResponseCode();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                JSONArray arr = null;
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                try {
                    arr = new JSONObject(content.toString()).optJSONObject(objectJSON).getJSONArray("data");
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
                for (Object o : arr) {
                    JSONArray test = (JSONArray) o;
                    imoexDataArray.add(accumulateData(test));
                }
            }
            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return imoexDataArray;
    }

    default public List<? extends ExternalData> mutliLoad(String urlString, String objectJSON) {
        int pageSize = 100;
        List<ExternalData> imoexDataArray = new ArrayList<>();
        for (int ind = 0; ind < 500 && imoexDataArray.addAll(load(urlString + "&start=" + ind * pageSize, objectJSON)); ++ind);
        return imoexDataArray;
    }

    default public List<? extends ExternalData> periodMultiLoad(String urlString, PeriodEntity entity, String objectJSON) {
        List<ExternalData> imoexDataArray = new ArrayList<>();
        for(LocalDate currentDate = LocalDate.parse(entity.getEffectiveFromDt(), dateFormat);
            !currentDate.isAfter(LocalDate.parse(entity.getEffectiveToDt(), dateFormat));
            currentDate = currentDate.plusDays(1)) {
            imoexDataArray.addAll(mutliLoad(urlString.concat("&date=").concat(currentDate.format(dateFormat)), objectJSON));
        }
        return imoexDataArray;
    }
}
