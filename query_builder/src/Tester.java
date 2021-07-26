/*
* Класс для тестирования функциональности основной программы
* через точку входа можно запустить прогон разлиных классов из проекта
* */
public class Tester {
    public static void main(String[] args){
//        testProgram();
//        testUpdateConditionComposer();
        testProgramOneFile();
//        testEntityParser();
    }
    /*
    * тестируем основной класс "Program"
    * */
    private static void testProgram(){
        String[] Args = new String[2];
        Args[0] = "..\\db\\script\\etl\\repl"; // - путь к каталогу с сущностями
        Args[1] = "src"; // - путь к каталогу с шаблонами
        Program.main(Args);
    }
    private static void testProgramOneFile(){
        String entityName    = "default_data_board";
        String sourceFileName   = "test\\model.sql";
        String destFileName     = "test\\destination.sql";
        Program.setPathToPatterns("src");

        Program.InitialisePatternVars(destFileName, "");
        Program.processEntityInFile(sourceFileName, entityName);

//        Program.
    }
    /*
    * тестируем вспомогательный класс testUpdateConditionComposer -
    * Он нужен для составления выражения в разделе DO UPDATE SET ...
    * см. файл "db\script\etl\repl\default_data_board\sql\default_data_board.sql"
    * */
    private static void testUpdateConditionComposer(){

        String fileName = "src\\UpdateConditionPattern.sql";
        UpdateConditionComposer test = new UpdateConditionComposer(fileName);
        System.out.println(test.getCondition("holiday_flag", "\t"));

    }

    private static void testEntityParser() {
        String fileName = "test\\model.sql";
        EntityParser ep = new EntityParser(fileName);
        ep.parseEntity();
        ep.printFields();
    }
}
