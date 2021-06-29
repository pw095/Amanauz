import java.io.*;
import java.util.Iterator;
import java.util.Calendar;
import java.util.GregorianCalendar;
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
  
    String sql = "DELETE FROM financial_statement";
    
    try {
      Statement stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void declare() {
  
    String sql = "INSERT INTO financial_statement(emitent_name, report_date, report_fs_item, currency, report_amnt) VALUES (?, ?, ?, ?, ?)";
    
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

  public void bindNumber(int Index, double arr) {

    try {
      pstmt.setLong(Index, (long) arr);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void bindDate(int Index, Date arr) {
  
    java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");
    bindString(Index, df.format(arr));
    
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



public class readFinStatement {
  public static void main(String[] args) {
  
    File srcFile = new File("/media/sf_financial_statement_data/ПАО «ГК «Самолет».xlsx");
    String fileName = srcFile.getName();
    String currency = null;
    int multiplier=0;
    InsertApp app = new InsertApp();
    app.clearTable();
    app.declare();

    Calendar[] ReportDt;
    java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");

    
    try (FileInputStream inputStream = new FileInputStream(srcFile)) {
    
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      Iterator<Sheet> sheetIterator = workbook.iterator();

      while(sheetIterator.hasNext()) {
      
        Sheet sheet = sheetIterator.next();
        Iterator<Row> rowIterator = sheet.iterator();
        ReportDt = new Calendar[sheet.getRow(0).getLastCellNum() - sheet.getRow(0).getFirstCellNum()-2];
        while (rowIterator.hasNext()) {

          Row row = rowIterator.next();
          Iterator<Cell> cellIterator = row.cellIterator();

          String FSItem = null;
          int Ind;
          while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            Ind = cell.getColumnIndex() - row.getFirstCellNum();
            if (row.getRowNum() == sheet.getFirstRowNum()) {
              if (Ind == 0) {
                switch(cell.getStringCellValue()) {
                  case "млн. руб.":
                    multiplier = 1_000_000;
                    currency = "RUB";
                    break;
                  case "тыс. руб.":
                    multiplier = 1_000;
                    currency = "RUB";
                    break;
                  case "руб.":
                    multiplier = 1;
                    currency = "RUB";
                    break;
                }
              } else if (Ind >= 2) {
                try {
                  ReportDt[Ind-2] = new GregorianCalendar((int) cell.getNumericCellValue(), Calendar.DECEMBER, 31);
                } catch (java.lang.IllegalStateException e) {
                  System.out.println("RowNumber = " + row.getRowNum());
                  System.out.println("ColumnIndex = " + cell.getColumnIndex());
                  System.out.println("FirstCellNum = " + row.getFirstCellNum());
                }
              }
            } else {
              if (Ind == 0) {
                FSItem = cell.getStringCellValue();
              }
              if (cell.getColumnIndex() >= 3 && cell.getCellType() != CellType.BLANK) {
                app.bindString(1, fileName.substring(0, fileName.lastIndexOf('.')));
                app.bindString(2, df.format(ReportDt[cell.getColumnIndex()-3].getTimeInMillis()));
                app.bindString(3, FSItem);
                app.bindString(4, currency);
                try {
                  app.bindNumber(5, (long) cell.getNumericCellValue() * multiplier);
                } catch (java.lang.IllegalStateException e) {
                  System.out.println("RowNumber = " + row.getRowNum());
                  System.out.println("ColumnIndex = " + cell.getColumnIndex());
                }
                app.bindBatch();
              }
            }
          }
        }
        app.commit();
      }
    } catch (IOException e) {
      System.out.println(e.getMessage());
    } catch (NullPointerException e) {
      System.out.println(e.getMessage());
    }
  }
}

