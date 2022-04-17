package Project1.Option;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class FileManipulation implements DataManipulation {
    private BufferedReader in;
    private BufferedWriter out;

    @Override
    public void openDatasource() {
        try {
            in = new BufferedReader(new FileReader("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\origin.csv"));
            StringBuilder sb = new StringBuilder();
            String s;
            in.readLine();
            while ((s = in.readLine()) != null) {
                sb.append(s).append("\n");
            }
            in.close();
            out = new BufferedWriter(new FileWriter("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            out.write(sb.toString());
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void closeDatasource() {

    }

    @Override
    public void insertOption(int totalCnt) {
        try {
            double t1 = System.currentTimeMillis(), t2, totalTime;
            String s = ",Studio Trigger,Asia,Japan,NULL,Internet,BJS0822,Poster,PosterKIK,50,1000,2013-10-3,2013-11-10,2013-11-11,Steven Edwards,Theo White,12140327,Male,25,13986643179,\n";
            out = new BufferedWriter(new FileWriter("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv", true));
            for (int i = 0; i < totalCnt; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("CSE%07d", 5000 + i)).append(s);
                out.write(sb.toString());
            }
            out.close();
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Insertion done, %d operations/sec\ntime: %.2fms\n\n", Math.round(totalCnt * 1000.0 / totalTime), totalTime);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void selectOption(int totalCnt) {
        try {
            double t1 = System.currentTimeMillis(), t2, totalTime;
            String[] arr;
            String lineContent;
            int contractNum;
            StringBuilder ans = new StringBuilder();
            LinkedList<String> list = new LinkedList<>();
            in = new BufferedReader(new FileReader("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            in.readLine();
            while ((lineContent = in.readLine()) != null) {
                list.add(lineContent);
            }
            in.close();
            for (int i = 0; i < totalCnt; i++) {
                for (String s : list) {
                    arr = s.split(",");
                    contractNum = Integer.parseInt(arr[0].substring(3, 10));
                    if (contractNum == i) {
                        ans.append(s).append("\n");
                    }
                }
            }
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Selection done, %d operations/sec\ntime: %.2fms\n\n", Math.round(totalCnt * 1000.0 / totalTime), totalTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateOption(int totalCnt) {
        try {
            double t1 = System.currentTimeMillis(), t2, totalTime;
            String lineContent;
            new StringBuilder();
            StringBuilder sb;
            String[] line;
            int contractNum;
            ArrayList<String> list = new ArrayList<>();
            in = new BufferedReader(new FileReader("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            in.readLine();
            while ((lineContent = in.readLine()) != null) {
                list.add(lineContent);
            }
            in.close();
            for (int i = 0; i < totalCnt; i++) {
                for (int j = 0; j < list.size(); j++) {
                    line = list.get(i).split(",");
                    contractNum = Integer.parseInt(line[0].substring(3, 10));
                    if (contractNum == 5000 + i) {
                        line[8] = "PosterFRANXX";
                    }
                    sb = new StringBuilder();
                    for (String s1 : line) {
                        sb.append(s1).append(",");
                    }
                    list.set(i, sb.toString());
                }
            }
            sb = new StringBuilder();
            for (String s : list) {
                sb.append(s).append("\n");
            }
            out = new BufferedWriter(new FileWriter("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            out.write(sb.toString());
            out.close();
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Update done, %d operations/sec\ntime: %.2fms\n\n", Math.round(totalCnt * 1000.0 / totalTime), totalTime);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteOption(int totalCnt) {
        try {
            double t1 = System.currentTimeMillis(), t2, totalTime;
            int contractNum;
            String lineContent;
            String[] line;
            new StringBuilder();
            StringBuilder sb;
            LinkedList<String> list = new LinkedList<>();
            in = new BufferedReader(new FileReader("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            in.readLine();
            while ((lineContent = in.readLine()) != null) {
                list.add(lineContent);
            }
            in.close();
            for (int i = 0; i < totalCnt; i++) {
                for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
                    line = iterator.next().split(",");
                    contractNum = Integer.parseInt(line[0].substring(3, 10));
                    if (contractNum == 5000 + i) {
                        iterator.remove();
                    }
                }
            }
            sb = new StringBuilder();
            for (String s : list) {
                sb.append(s).append("\n");
            }
            out = new BufferedWriter(new FileWriter("D:\\Program\\Idea\\CS307_2022spring\\src\\Project1\\contract_info.csv"));
            out.write(sb.toString());
            out.close();
            t2 = System.currentTimeMillis();
            totalTime = t2 - t1;
            System.out.printf("Delete done, %d operations/sec\ntime: %.2fms\n\n", Math.round(totalCnt * 1000.0 / totalTime), totalTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
