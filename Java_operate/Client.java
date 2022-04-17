package Project1.Option;

public class Client {
    public static void main(String[] args) {
        try {
            DataManipulation dmData = new DataFactory().createDataManipulation("database"),
                    dmFile = new DataFactory().createDataManipulation("file");
            int repeatTimes = 1_000;

//            System.out.println("Database:");
//            dmData.openDatasource();
//            dmData.insertOption(repeatTimes);
//            dmData.selectOption(repeatTimes);
//            dmData.updateOption(repeatTimes);
//            dmData.deleteOption(repeatTimes);
//            dmData.closeDatasource();

            System.out.println("File IO:");
            dmFile.openDatasource();
            dmFile.insertOption(repeatTimes);
            dmFile.selectOption(repeatTimes);
            dmFile.updateOption(repeatTimes);
            dmFile.deleteOption(repeatTimes);
            dmFile.closeDatasource();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
