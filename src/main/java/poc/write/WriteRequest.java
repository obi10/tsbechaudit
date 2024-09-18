package poc.write;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import java.util.List;

public class WriteRequest implements Runnable {

    private Connection conn;
    private final int maxDuration;
    private boolean isDone = false;
    private final int batchSize;
    private BufferedWriter logger;
    private final List<List<String>> data;

    private int count = 0;
    private int duration = 0;

    public WriteRequest(int threadNumber, Connection conn, int maxDurationSeconds, int batchSize){

        this.conn = conn;
        this.maxDuration = maxDurationSeconds;
        this.batchSize = batchSize;

        try {
            logger = new BufferedWriter(new FileWriter("/home/opc/Documents/tsbechaudit/log/write_thread_" + threadNumber + ".log"));
        } catch (Exception e) {
            System.out.println("Error on creating log file: " + e);
            System.exit(1);
        }
        data = new ArrayList<>();//list of lists to store data
        String csvFile = "/home/opc/IdeaProjects/tsbechaudit/src/main/java/poc/devices.csv"; // Replace with your CSV file path
        try {
            FileReader fr = new FileReader(csvFile);
            BufferedReader br = new BufferedReader(fr);
            //Reading until we run out of lines
            try {
                String line = br.readLine();
                while(line != null)
                {
                    List<String> lineData = Arrays.asList(line.split(","));//splitting lines
                    data.add(lineData);
                    try {
                        line = br.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void batchUpdate(int batchSize) {
        long start, end;

        try {
            conn.setAutoCommit(false);

            int index = getRandomNumber(1, data.size()-1);
            String[] randomDeviceData = data.get(index).toString().split(",");
            String update = "UPDATE device SET version = ?, app_version = ?, application_id = ?, date_created = ?, last_updated = ?, os_version = ?, platform = ?, token = ?, type = ?, user_id = ?, lang = ?, site_id= ?, timezone = ?, status = ?, advertising_id = ? where device_id = ? and market_place = ?";

            while (true) {
                Instant startBatch = Instant.now();
                PreparedStatement prepStmt = conn.prepareStatement(update);
                for (int i = 0; i < batchSize; i++) {
                    prepStmt.setString(1, randomDeviceData[1]);
                    prepStmt.setString(2, randomDeviceData[2]);
                    prepStmt.setString(3, randomDeviceData[3]);
                    prepStmt.setTimestamp(4, getTS());
                    prepStmt.setTimestamp(5, getTS());
                    prepStmt.setString(6, randomDeviceData[6]);
                    prepStmt.setString(7,randomDeviceData[7]);
                    prepStmt.setString(8, randomDeviceData[8]);
                    prepStmt.setString(9, randomDeviceData[9]);
                    prepStmt.setInt(10, getUserID());
                    prepStmt.setString(11, randomDeviceData[11]);
                    prepStmt.setString(12, randomDeviceData[12]);
                    prepStmt.setString(13, randomDeviceData[13]);
                    prepStmt.setString(14, randomDeviceData[14]);
                    prepStmt.setString(15, randomDeviceData[15]);
                    prepStmt.setString(16, randomDeviceData[0]);
                    prepStmt.setString(17, randomDeviceData[14]);
                    prepStmt.addBatch();
                }

                try {
                    int[] rows = prepStmt.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println("error en batchUpdate ThreadWrite: " + e);
                }
                Instant endBatch = Instant.now();
                writeLog("BatchUpd1," + batchSize + "," + Duration.between(startBatch, endBatch).toMillis());
                prepStmt.close();
            }
        } catch (Exception e) {
            System.out.println("error en batch update method - ThreadWrite: " + e);
        }
    }

    public void update(String[]  randomDeviceData) {
        PreparedStatement prepStmt;
        try {
            conn.setAutoCommit(false);

            String update = "UPDATE device SET version = ?, app_version = ?, application_id = ?, date_created = ?, last_updated = ?, os_version = ?, platform = ?, token = ?, type = ?, user_id = ?, lang = ?, site_id= ?, timezone = ?, status = ?, advertising_id = ? where device_id = ? and market_place = ?";
            //System.out.println("randomDeviceData[1]:---" + randomDeviceData[1]);
            //System.out.println("randomDeviceData[0]:---" + randomDeviceData[0].substring(1));
            //System.out.println("randomDeviceData[14]:---" + randomDeviceData[14].substring(1));

            int numUPd = 0;

            Instant start = Instant.now();
            prepStmt = conn.prepareStatement(update); // prepare one time run
            prepStmt.setString(1, randomDeviceData[1].trim());
            prepStmt.setString(2, randomDeviceData[2].trim());
            prepStmt.setString(3, randomDeviceData[3].trim());
            prepStmt.setTimestamp(4, getTS());
            prepStmt.setTimestamp(5, getTS());
            prepStmt.setString(6, randomDeviceData[6].trim());
            prepStmt.setString(7,randomDeviceData[7].trim());
            prepStmt.setString(8, randomDeviceData[8].trim());
            prepStmt.setString(9, randomDeviceData[9].trim());
            prepStmt.setInt(10, getUserID());
            prepStmt.setString(11, randomDeviceData[11].trim());
            prepStmt.setString(12, randomDeviceData[12].trim());
            prepStmt.setString(13, randomDeviceData[13].trim());
            prepStmt.setString(14, randomDeviceData[15].trim());
            prepStmt.setString(15, randomDeviceData[16].trim());
            prepStmt.setString(16, randomDeviceData[0].substring(1));
            prepStmt.setString(17, randomDeviceData[14].substring(1));

            try {
                numUPd = prepStmt.executeUpdate();
                conn.commit();
                prepStmt.close();
            } catch (SQLException e) {
                System.out.println("error en update ThreadWrite: " + e);
            }
            Instant end = Instant.now();
            writeLog("Upd1," + numUPd + "," + Duration.between(start, end).toMillis());

        } catch (Exception e) {
            System.out.println("error en update ThreadWrite: " + e);
        }
    }

    public void insert(String[]  randomJSONData) {
        PreparedStatement prepStmt;
        try {
            conn.setAutoCommit(false);

            String insert = "UPDATE device SET version = ?, app_version = ?, application_id = ?, date_created = ?, last_updated = ?, os_version = ?, platform = ?, token = ?, type = ?, user_id = ?, lang = ?, site_id= ?, timezone = ?, status = ?, advertising_id = ? where device_id = ? and market_place = ?";
            //System.out.println("randomDeviceData[1]:---" + randomDeviceData[1]);
            //System.out.println("randomDeviceData[0]:---" + randomDeviceData[0].substring(1));
            //System.out.println("randomDeviceData[14]:---" + randomDeviceData[14].substring(1));

            int numUPd = 0;

            Instant start = Instant.now();
            prepStmt = conn.prepareStatement(update); // prepare one time run
            prepStmt.setString(1, randomDeviceData[1].trim());
            prepStmt.setString(2, randomDeviceData[2].trim());
            prepStmt.setString(3, randomDeviceData[3].trim());
            prepStmt.setTimestamp(4, getTS());
            prepStmt.setTimestamp(5, getTS());
            prepStmt.setString(6, randomDeviceData[6].trim());
            prepStmt.setString(7,randomDeviceData[7].trim());
            prepStmt.setString(8, randomDeviceData[8].trim());
            prepStmt.setString(9, randomDeviceData[9].trim());
            prepStmt.setInt(10, getUserID());
            prepStmt.setString(11, randomDeviceData[11].trim());
            prepStmt.setString(12, randomDeviceData[12].trim());
            prepStmt.setString(13, randomDeviceData[13].trim());
            prepStmt.setString(14, randomDeviceData[15].trim());
            prepStmt.setString(15, randomDeviceData[16].trim());
            prepStmt.setString(16, randomDeviceData[0].substring(1));
            prepStmt.setString(17, randomDeviceData[14].substring(1));

            try {
                numUPd = prepStmt.executeUpdate();
                conn.commit();
                prepStmt.close();
            } catch (SQLException e) {
                System.out.println("error en update ThreadWrite: " + e);
            }
            Instant end = Instant.now();
            writeLog("Upd1," + numUPd + "," + Duration.between(start, end).toMillis());

        } catch (Exception e) {
            System.out.println("error en update ThreadWrite: " + e);
        }
    }

    public void batchInsert(int batchSize) {
        long start, end;

        try {
            conn.setAutoCommit(false);

            int index = getRandomNumber(1, data.size()-1);
            String[] randomDeviceData = data.get(index).toString().split(",");
            String update = "UPDATE device SET version = ?, app_version = ?, application_id = ?, date_created = ?, last_updated = ?, os_version = ?, platform = ?, token = ?, type = ?, user_id = ?, lang = ?, site_id= ?, timezone = ?, status = ?, advertising_id = ? where device_id = ? and market_place = ?";

            while (true) {
                Instant startBatch = Instant.now();
                PreparedStatement prepStmt = conn.prepareStatement(update);
                for (int i = 0; i < batchSize; i++) {
                    prepStmt.setString(1, randomDeviceData[1]);
                    prepStmt.setString(2, randomDeviceData[2]);
                    prepStmt.setString(3, randomDeviceData[3]);
                    prepStmt.setTimestamp(4, getTS());
                    prepStmt.setTimestamp(5, getTS());
                    prepStmt.setString(6, randomDeviceData[6]);
                    prepStmt.setString(7,randomDeviceData[7]);
                    prepStmt.setString(8, randomDeviceData[8]);
                    prepStmt.setString(9, randomDeviceData[9]);
                    prepStmt.setInt(10, getUserID());
                    prepStmt.setString(11, randomDeviceData[11]);
                    prepStmt.setString(12, randomDeviceData[12]);
                    prepStmt.setString(13, randomDeviceData[13]);
                    prepStmt.setString(14, randomDeviceData[14]);
                    prepStmt.setString(15, randomDeviceData[15]);
                    prepStmt.setString(16, randomDeviceData[0]);
                    prepStmt.setString(17, randomDeviceData[14]);
                    prepStmt.addBatch();
                }

                try {
                    int[] rows = prepStmt.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println("error en batchUpdate ThreadWrite: " + e);
                }
                Instant endBatch = Instant.now();
                writeLog("BatchUpd1," + batchSize + "," + Duration.between(startBatch, endBatch).toMillis());
                prepStmt.close();
            }
        } catch (Exception e) {
            System.out.println("error en batch update method - ThreadWrite: " + e);
        }
    }

    @Override
    public void run() {
        long start, end;
        start = System.currentTimeMillis();
        count=0;

        while (true) {
            int index = getRandomNumber(1, data.size()-1);
            String[] randomDevice = data.get(index).toString().split(",");
            if (batchSize > 1){
                batchInsert(batchSize);
                count = count + batchSize;
            }
            else{
                insert(randomDevice);
                count++;
            }
            end = System.currentTimeMillis();
            duration = (int) (end - start) / 1000;

        }
    }

    public void closeWriter() {
        try {
            logger.close();
        } catch (Exception e) {
            System.out.println("Error closing log file: " + e);
            System.exit(1);
        }
    }
    public Timestamp getTS() {
        Timestamp iniTS = Timestamp.valueOf("2024-09-01 00:00:00");
        Timestamp endTS = Timestamp.valueOf("2024-12-31 23:59:59");
        long randomTimestampInMillis = ThreadLocalRandom.current().nextLong(iniTS.getTime(), endTS.getTime() + 1);
        return new Timestamp(randomTimestampInMillis);
    }

    public int getUserID() {
        int min = 1017154300;
        int max = 1017156300;

        return getRandomNumber(min,max);
    }

    public int getRandomNumber(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public boolean isDone() {
        return isDone;
    }

    public int getCount() {
        return count;
    }

    public int getDuration() {
        return duration;
    }

    public void writeLog(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        try {
            logger.write(sdf.format(new Date().getTime()) + "," + str + "\n");
        } catch (Exception e) {
            System.out.println("Error on writing to the log file: " + e);
            System.exit(1);
        }
    }
}
