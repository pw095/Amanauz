import java.io.*;
import java.util.Iterator;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;

public class readExcel1 {
  public static void main(String[] args) {
    File srcFile = new File("/media/sf_shared_folder/Самолет 2015 -- 2018.xlsx");
    String fileName = srcFile.getName();
    int multiplier=0;
    try (FileInputStream inputStream = new FileInputStream(srcFile)) {
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      Iterator<Sheet> sheetIterator = workbook.iterator();

      while(sheetIterator.hasNext()) {
      
        Sheet sheet = sheetIterator.next();
        System.out.println(sheet.getSheetName());
        Iterator<Row> rowIterator = sheet.iterator();
        ReportDt = new Calendar[sheet.getRow(0).getLastCellNum() - sheet.getRow(0).getFirstCellNum()-2];
        while (rowIterator.hasNext()) {

          Row row = rowIterator.next();
          Iterator<Cell> cellIterator = row.cellIterator();

          String FSIndex = null;
          int Ind;
          while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            Ind = cell.getColumnIndex() - row.getFirstCellNum();
            if (row.getRowNum() == sheet.getFirstRowNum()) {
              if (Ind == 0) {
                switch(cell.getStringCellValue()) {
                  case "млн. руб.":
                    multiplier = 1_000_000;
                    break;
                  case "тыс. руб.":
                    multiplier = 1_000;
                    break;
                  case "руб.":
                    multiplier = 1;
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
                FSIndex = cell.getStringCellValue();
              }
              if (cell.getColumnIndex() >= 3 && cell.getCellType() != CellType.BLANK) {
                app.bindString(1, fileName.substring(0, fileName.lastIndexOf('.')));
                app.bindString(2, df.format(ReportDt[cell.getColumnIndex()-3].getTimeInMillis()));
                app.bindString(3, "YEAR");
                app.bindString(4, FSIndex);
              System.out.println(fileName.substring(0, fileName.lastIndexOf('.')) + " " +  df.format(ReportDt[cell.getColumnIndex()-3].getTimeInMillis()) + " " + str + " " + (long) cell.getNumericCellValue()* multiplier);
              }
            }
          }
        }
        app.commit();
      }
      
      /*
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      XSSFSheet sheet = workbook.getSheetAt(1);

      Calendar[] ReportDt;
      java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");
      
      Iterator<Row> rowIterator = sheet.iterator();
      ReportDt = new Calendar[sheet.getRow(0).getLastCellNum() - sheet.getRow(0).getFirstCellNum()-2];
      while (rowIterator.hasNext()) {

        Row row = rowIterator.next();
        Iterator<Cell> cellIterator = row.cellIterator();


        String str = null;
        int Ind;
        while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          Ind = cell.getColumnIndex() - row.getFirstCellNum();
          if (row.getRowNum() == sheet.getFirstRowNum()) {
            if (Ind == 0) {
              switch(cell.getStringCellValue()) {
                case "млн. руб.":
                  multiplier = 1_000_000;
                  break;
                case "тыс. руб.":
                  multiplier = 1_000;
                  break;
                case "руб.":
                  multiplier = 1;
                  break;
              }
              // 
            } else if (Ind >= 2) {
              ReportDt[Ind-2] = new GregorianCalendar((int) cell.getNumericCellValue(), Calendar.DECEMBER, 31);
            }
          } else {
            if (Ind == 0) {
              str = cell.getStringCellValue();
            }
            if (cell.getColumnIndex() >= 3 && cell.getCellType() != CellType.BLANK) {
              System.out.println(fileName.substring(0, fileName.lastIndexOf('.')) + " " +  df.format(ReportDt[cell.getColumnIndex()-3].getTimeInMillis()) + " " + str + " " + (long) cell.getNumericCellValue()* multiplier);
            }
          }
        }
      }
      */
    } catch(IOException e) {
      System.out.println("An I/O Error Occurred");
    }
  }
}
