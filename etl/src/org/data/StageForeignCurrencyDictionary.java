package org.data;

import org.json.JSONArray;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;
import static org.data.ImoexWebSiteEntity.ImoexData;

public class StageForeignCurrencyDictionary extends CbrWebSiteEntity {

    static class StageForeignCurrencyDictionaryData extends ImoexWebSiteEntity.ImoexData {
        String id;
        String name;
        String engName;
        long   nominal;
        String parentCode;
        String isoNumCode;
        String isoCharCode;

        public StageForeignCurrencyDictionaryData(String id,
                                                  String name,
                                                  String engName,
                                                  long   nominal,
                                                  String parentCode,
                                                  String isoNumCode,
                                                  String isoCharCode) {
            this.id = id;
            this.name = name;
            this.engName = engName;
            this.nominal = nominal;
            this.parentCode = parentCode;
            this.isoNumCode = isoNumCode;
            this.isoCharCode = isoCharCode;
        }

    }

    public StageForeignCurrencyDictionary(long flowLoadId) {
        super(flowLoadId, "foreign_currency_dictionary");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
//        http://iss.moex.com/iss/engines/stock/markets/shares/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,BOARDID,SHORTNAME,PREVPRICE,LOTSIZE,FACEVALUE,STATUS,BOARNAME,DECIMALS,SECNAME,REMARKS,MARKETCODE,INSTRID,MINSTEP,PREVWAPRICE,FACEUNIT,PREVDATE,ISSUESIZE,ISIN,LATNAME,REGNUMBER,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,CURRENCYID,SECTYPE,LISTLEVEL,SETTLEDATE
        String urlStringRaw = "http://www.cbr.ru/scripts/XML_valFull.asp?d=";
        String urlStringData;
/*        String urlColumnList = "/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,BOARDID,SHORTNAME,PREVPRICE,LOTSIZE,FACEVALUE,STATUS,BOARNAME,DECIMALS,SECNAME,REMARKS,MARKETCODE,INSTRID,MINSTEP,PREVWAPRICE,FACEUNIT,PREVDATE,ISSUESIZE,ISIN,LATNAME,REGNUMBER,PREVLEGALCLOSEPRICE,PREVADMITTEDQUOTE,CURRENCYID,SECTYPE,LISTLEVEL,SETTLEDATE";
        String urlStringMeta = null;*/
        urlStringData = urlStringRaw.concat("0");

        try {
            noHistoryCbrLoad(stmtUpdate, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        urlStringData = urlStringRaw.concat("1");

        try {
            noHistoryCbrLoad(stmtUpdate, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }
/*
        urlStringData = urlStringRaw.concat("bonds").concat(urlColumnList);

        try {
//            completeLoad(stmtUpdate, urlStringMeta, urlStringData);
//            singleIterationLoad(stmtUpdate, urlStringData, "data");
            noHistoryLoad(stmtUpdate, urlStringData, "securities", false);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    @Override
    List<? extends ImoexData> parseXML(Document document) {

        List<StageForeignCurrencyDictionaryData> dataList = new ArrayList<>();
        NodeList nodeList = document.getDocumentElement().getChildNodes();

        String name = null;
        String engName = null;
        long   nominal = -1;
        String parentCode = null;
        String isoNumCode = null;
        String isoCharCode = null;

        for (int ii=0; ii < nodeList.getLength(); ii++) {
            Node nodeItem = nodeList.item(ii);
            NodeList itemList = nodeItem.getChildNodes();
            for (int jj=0; jj < itemList.getLength(); jj++) {
                Node itemInfo = itemList.item(jj);
                if (itemInfo.getNodeType() != Node.TEXT_NODE) {
                    Node item = itemInfo.getChildNodes().item(0);
                    String currentItem = item == null ? null : item.getTextContent();
                    switch(jj) {
                        case 0:
                            name = currentItem;
                            break;
                        case 1:
                            engName = currentItem;
                            break;
                        case 2:
                            nominal = Long.valueOf(currentItem);
                            break;
                        case 3:
                            parentCode = currentItem;
                            break;
                        case 4:
                            isoNumCode = currentItem;
                            break;
                        case 5:
                            isoCharCode = currentItem;
                            break;
                    }
                    System.out.println(itemInfo.getNodeName() + " " + currentItem);
                }
            }
            dataList.add(new StageForeignCurrencyDictionaryData(nodeItem.getAttributes().getNamedItem("ID").getTextContent(),
                                                                name,
                                                                engName,
                                                                nominal,
                                                                parentCode,
                                                                isoNumCode,
                                                                isoCharCode));
        }
        return dataList;
    }



    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageForeignCurrencyDictionaryData jter = (StageForeignCurrencyDictionaryData) iter;

                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getEntityStartLoadTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.id);
                stmtUpdate.setString(4, jter.name);
                stmtUpdate.setString(5, jter.engName);
                stmtUpdate.setLong(6, jter.nominal);
                stmtUpdate.setString(7, jter.parentCode);
                stmtUpdate.setString(8, jter.isoNumCode);
                stmtUpdate.setString(9, jter.isoCharCode);

                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
//            System.out.println("Length: " + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
