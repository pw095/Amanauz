package org.data;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.List;

public interface CbrSourceEntity extends ExternalSourceEntity {
    public abstract List<? extends ExternalData> accumulateData(Document document);
    public DateTimeFormatter cbrPutDateFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    public DateTimeFormatter cbrGetDateFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    default public Document load(String urlString) {
        Document document = null;
        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.getResponseCode();
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = documentBuilder.parse(conn.getInputStream());
            conn.disconnect();
            return document;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return document;
    }
}
