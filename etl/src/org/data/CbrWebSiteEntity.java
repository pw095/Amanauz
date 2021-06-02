package org.data;
import org.meta.MetaLayer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.util.AuxUtil.dateFormat;

public abstract class CbrWebSiteEntity extends AbstractEntity {

    public static final DateTimeFormatter cbrPutDateFormat = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    public static final DateTimeFormatter cbrGetDateFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    String previousEffectiveToDt;
    private String effectiveFromDt;
    private String effectiveToDt;

    public String getPreviousEffectiveToDt() {
        return this.previousEffectiveToDt;
    }
    public void setPreviousEffectiveToDt(String previousEffectiveToDt) {
        this.previousEffectiveToDt = previousEffectiveToDt;
    }

    public String getEffectiveFromDt() {
        return this.effectiveFromDt;
    }
    public void setEffectiveFromDt(String effectiveFromDt) {
        this.effectiveFromDt = effectiveFromDt;
    }

    public String getEffectiveToDt() {
        return this.effectiveToDt;
    }
    public void setEffectiveToDt(String effectiveToDt) {
        this.effectiveToDt = effectiveToDt;
    }

    public CbrWebSiteEntity(long flowLoadId, String entityCode) {
        super(flowLoadId, MetaLayer.STAGE, entityCode);
    }

    public void historyCbrLoad(PreparedStatement stmtUpdate, String urlString) throws Exception {

        Document document = coreCbrLoad(urlString
                .concat("&date_req1=")
                .concat(LocalDate.parse(this.getEffectiveFromDt(), dateFormat).format(cbrPutDateFormat))
                .concat("&date_req2=")
                .concat(LocalDate.parse(this.getEffectiveToDt(), dateFormat).format(cbrPutDateFormat)));

        saveData(stmtUpdate, parseXML(document));

    }

    public void noHistoryCbrLoad(PreparedStatement stmtUpdate, String urlString) throws Exception {

        Document document = coreCbrLoad(urlString);

        saveData(stmtUpdate, parseXML(document));

    }

    public Document coreCbrLoad(String urlString) throws Exception {
        URL url = new URL(urlString);
        System.out.println(url);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.getResponseCode();
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = documentBuilder.parse(conn.getInputStream());

        return document;

    }

    abstract List<? extends ImoexWebSiteEntity.ImoexData> parseXML(Document document);
    abstract void saveData(PreparedStatement stmtUpdate, List<? extends ImoexWebSiteEntity.ImoexData> dataArray);
}
