import java.io.*;
import java.util.Iterator;
import java.math.*;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.sql.*;

class InsertApp {

  private Connection conn;
  private PreparedStatement pstmt;
  
  private Connection connect() {
    String url = "jdbc:sqlite:/media/sf_db/stage.db";
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return conn;
  }

  public void disconnect() {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  
  InsertApp() {
    conn = connect();
  }

  public void clearTable() {
  
    String sql = "DELETE FROM master_data_ref_fin_statement";
    
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void declare() {
  
    String sql = "INSERT INTO master_data_ref_fin_statement(level, leaf_code, code, parent_leaf_code, parent_code, name) VALUES (?, ?, ?, ?, ?, ?);";

    
    try {
      pstmt = conn.prepareStatement(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  


  public void bindString(int Index, String arr) {

    try {
      if (arr.isEmpty() || arr == null) {
        pstmt.setNull(Index, Types.CHAR);
      } else {
        pstmt.setString(Index, arr);
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

  }
  
  public void bindDate(int Index, Date arr) {
  
    java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");
    bindString(Index, df.format(arr));
    
  }

  public void bindNumber(int Index, double arr) {

    try {
      pstmt.setLong(Index, (long) arr);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void bindBatch() {

    try {
      pstmt.addBatch();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

  }

  public void commit() {

    try {
      pstmt.executeBatch();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

  }
  
}



public class readMasterDataRefFinStatement {
  public static void main(String[] args) {
  
    File srcFile = new File("/media/sf_master_data/MasterDataRefFinStatement.xlsx");
    String fileName = srcFile.getName();
    String[] attrValue = new String[5];
    InsertApp app = new InsertApp();
    app.clearTable();
    app.declare();
    
    java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");
    
    
    try (FileInputStream inputStream = new FileInputStream(srcFile)) {
    
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      Iterator<Sheet> sheetIterator = workbook.iterator();
      while(sheetIterator.hasNext()) {      
        Sheet sheet = sheetIterator.next();
        Iterator<Row> rowIterator = sheet.iterator();
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          if(row.getRowNum() == 0) {
            continue;
          }
          Iterator<Cell> cellIterator = row.cellIterator();
          double attrLevel = -1;
          for(int i=0; i<attrValue.length; i++) {
            attrValue[i] = new String();
          }
          while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            int colInd = cell.getColumnIndex();
            CellType ct = cell.getCellType();
            String cellValue;
            switch (ct) {
              case BLANK:
                continue;
              default:
                if (colInd == 0) {
                  attrLevel = cell.getNumericCellValue();
//                  java.util.Date cellDt = cell.getDateCellValue();
//                  cellValue = df.format(cellDt.getTime());
                } else {
                  switch (ct) {
                    case NUMERIC:
                      long cellLong = (long) cell.getNumericCellValue();
                      cellValue = Long.valueOf(cellLong).toString();
                      break;
                    default:
                      cellValue = cell.getStringCellValue();
                      break;
                  }
                  attrValue[colInd-1] = cellValue;
                }
            }
          }
          app.bindNumber(1, attrLevel);
          for(int i=0; i<attrValue.length; i++) {
            app.bindString(i+2, attrValue[i]);
          }
          app.bindBatch();
        }
      }
      app.commit();
      app.disconnect();
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } catch (NullPointerException e) {
          System.out.println("3");
      System.out.println(e.getMessage());
    }
  }
}

