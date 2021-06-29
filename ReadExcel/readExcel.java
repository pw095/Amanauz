import java.io.*;
import java.util.Iterator;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;

public class readExcel {
  public static void main(String[] args) throws IOException {
    FileInputStream inputStream = new FileInputStream(new File("/media/sf_shared_folder/prod.xlsx"));
    
    
    XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
    XSSFSheet sheet = workbook.getSheetAt(0);
    String SCellValue = new String();
    char[] c;
    int ColumnIndex;
    
    Iterator<Row> rowIterator = sheet.iterator();
    try (FileWriter outputFile = new FileWriter("prod.txt"))
    {
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      ColumnIndex = 0;
      // Get iterator to all cells of current row
      Iterator<Cell> cellIterator = row.cellIterator();

      while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();

          // Change to getCellType() if using POI 4.x
          CellType cellType = cell.getCellType();

          switch (cellType) {
          /*
          case _NONE:
              System.out.print("");
              System.out.print("\t");
              break;
          case BOOLEAN:
              System.out.print(cell.getBooleanCellValue());
              System.out.print("\t");
              break;
          case BLANK:
              System.out.print("");
              System.out.print("\t");
              break;
          case FORMULA:
              // Formula
              System.out.print(cell.getCellFormula());
              System.out.print("\t");
               
              FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
              // Print out value evaluated by formula
              System.out.print(evaluator.evaluate(cell).getNumberValue());
              break;
          case NUMERIC:
              System.out.print(cell.getNumericCellValue());
              System.out.print("\t");
              break;
              */
          case STRING:
              SCellValue = cell.getStringCellValue();
              ColumnIndex = cell.getColumnIndex();
              if (ColumnIndex != 0) {
                outputFile.write('\t');
              }
              c = new char[SCellValue.length()];
              SCellValue.getChars(0, SCellValue.length(), c, 0);
              outputFile.write(c);
              break;
          case ERROR:
              System.out.print("!");
              System.out.print("\t");
              break;
          }
//          fout.write(SCellValue);
//          System.out.println(SCellValue);
      }
      for(; ColumnIndex<5; ColumnIndex++) {
//        outputFile.write(ColumnIndex);
//        System.out.println(ColumnIndex);
        outputFile.write('\t');
      }
      outputFile.write('\n');
//      System.out.println("");
    }
    } catch(IOException e) {
      System.out.println("An I/O Error Occurred");
    }
  }
}
