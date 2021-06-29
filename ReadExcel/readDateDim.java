import java.io.*;
import java.util.Iterator;
import java.util.Date;
//import static java.text.DateFormat.DateFormat;
import java.math.*;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;

import java.sql.*;

class InsertApp {

  private Connection conn;
  private PreparedStatement pstmt;
  
  private Connection connect() {
    String url = "jdbc:sqlite:/media/sf_shared_folder/chinook.db";
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return conn;
  }

  InsertApp() {
    conn = connect();
  }


/*
  public void execute_truncate() {
   
    String sql = "TRUNCATE TABLE calendar_stg;";
    PreparedStatement pstmt = null;
    
    try {
      pstmt = conn.prepareStatement(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    
    try {
      pstmt.execute();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        System.out.println(e.getMessage());
      }
    }
  }
*/


  public void declare() {
  
    String sql = "INSERT INTO calendar_stg(full_date, day_of_week, day_num_in_month, day_num_in_year, day_name, day_abbrev, weekday_flag, week_num_in_year, week_begin_date, month, month_name, month_abbrev, quarter, year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    
    try {
      pstmt = conn.prepareStatement(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  


  public void bindString(int Index, String arr) {

    try {
      if (arr.isEmpty()) {
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



public class readDateDim {
  public static void main(String[] args) {
  
    int RowIndex = 0;
    try (FileInputStream inputStream = new FileInputStream(new File("/media/sf_shared_folder/DateDimension.xlsx"))) {
    
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      XSSFSheet sheet = workbook.getSheetAt(0);
      
      String CellValueString = new String();
      long CellValueLong = -1;
      
      int ValueIndex;
      InsertApp app = new InsertApp();
      
      app.declare();

      
      Iterator<Row> rowIterator = sheet.iterator();
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        if(RowIndex++ == 0) {
          continue;
        }
        
        ValueIndex = 0;
        Iterator<Cell> cellIterator = row.cellIterator();

        while (cellIterator.hasNext()) {

          Cell cell = cellIterator.next();
          if (cell == null) {
            return;
          }
          CellType cellType = cell.getCellType();

          switch (cellType) {
/*
            case NUMERIC:
                CellValueLong = (long) cell.getNumericCellValue();
                break;
            case STRING:
                CellValueString = cell.getStringCellValue();
                break;
*/
            case ERROR:
                System.out.println("Error");
                break;
            case _NONE:
                System.out.println("None");
                break;
            case BLANK:
                System.out.println("Blank");
                System.out.println("RowIndex = " + RowIndex);
                return;
/*
            case BOOLEAN:
                System.out.println("Bool");
                break;

            case FORMULA:
                System.out.println("Formula");
                break;
            default:
                System.out.println("!!!<>!!!");
                break;
                */
          }
/*
          System.out.println("ValueIndex = " + ValueIndex);
          System.out.println(CellValueLong + " " + CellValueString);
*/
          switch(ValueIndex++) {
            case 0: // full_date
              app.bindDate(ValueIndex, cell.getDateCellValue());
              break;
            case 1: // day_of_week
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 2: // day_num_in_month
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 3: // day_num_in_year
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 4: // day_name
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 5: // day_abbrev
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 6: // weekday_flag
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 7: // week_num_in_year
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 8: // week_begin_date
              app.bindDate(ValueIndex, cell.getDateCellValue());
              break;
            case 9: // month
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 10: // month_name
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 11: // month_abbrev
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 12: // quarter
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 13: // year
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
          }
        }
        app.bindBatch();
        if (RowIndex%1000 == 0) {
          app.commit();
        }
      }
      app.commit();

    } catch (IOException e) {
      System.out.println(e.getMessage());
    } catch (NullPointerException e) {
      System.out.println("RowIndex = " + RowIndex);
      System.out.println(e.getMessage());
    }
  }
}

