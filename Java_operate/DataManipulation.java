package Project1.Option;

public interface DataManipulation {

    public void openDatasource();
    public void closeDatasource();
    public void insertOption(int totalCnt);
    public void selectOption(int totalCnt);
    public void updateOption(int totalCnt);
    public void deleteOption(int totalCnt);
}
