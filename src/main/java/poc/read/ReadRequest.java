package poc.read;

import java.io.*;
import java.sql.*;
import java.time.Instant;
import java.time.Duration;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class ReadRequest implements Runnable {

    private final Connection conn;
    private final int maxDuration;
    private boolean isDone = false;
    private int count = 0;
    private int duration;
    private BufferedWriter logger;

    public ReadRequest(int threadNumber, Connection conn, int maxDurationSeconds) {

        this.conn = conn;
        this.maxDuration = maxDurationSeconds;

        try {
            logger = new BufferedWriter(new FileWriter("log/read_thread_" + threadNumber + ".log"));
        } catch (Exception e) {
            System.out.println("Error on creating log file: " + e);
            System.exit(1);
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


    @Override
    public void run() {

        long start, end;
        start = System.currentTimeMillis();
        while (true) {


            count++;

            end = System.currentTimeMillis();
            duration = (int) (end - start) / 1000;

            if (duration > maxDuration) {
                isDone = true;
                closeWriter();
                break;
            }
        }
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

    public void query1(String inputDevice, String inputMarketplace) throws SQLException {

        String QUERY1 = "select device_id, version, app_version, application_id, date_created, last_updated, os_version, platform, token, type, user_id, lang, site_id, market_place, timezone, status, advertising_id from device where device_id = ? and market_place = ?";
        int rowCount = 0;
        Instant start = Instant.now();
        PreparedStatement stmt = conn.prepareStatement(QUERY1);
        stmt.setString(1, inputDevice);
        stmt.setString(2, inputMarketplace);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            rs.getString(1);
            rowCount++;
        }
        //System.out.println("rowCount  =" + rowCount);
        Instant end = Instant.now();
        stmt.close();
        rs.close();
        writeLog("Qry1," + rowCount + "," + Duration.between(start, end).toMillis());

    }

    private int getRandomNumInRange(int max) {

        if (1 >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - 1) + 1) + 1;
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

}
