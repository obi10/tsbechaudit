package poc.read;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.text.SimpleDateFormat;

import java.io.*;
import java.sql.*;
import java.util.*;

import oracle.jdbc.OracleType;

public class ReadRequest implements Runnable {

    private final int threadNumber;
    private final Connection conn;
    private final int maxDuration;
    private final int vectorDim;

    private BufferedWriter logger;
    private boolean isDone = false;
    private int duration = 0;

    public ReadRequest(int threadNumber, Connection conn, int maxDurationSeconds, int vectorDim) {
        this.threadNumber = threadNumber;
        this.conn = conn;
        this.maxDuration = maxDurationSeconds;
        this.vectorDim = vectorDim;
    }

/*
    @Override
    public void run() {

        try {
            logger = new BufferedWriter(new FileWriter("log/read_thread_" + threadNumber + ".log"));
        } catch (Exception e) {
            System.out.println("Error on creating log file: " + e);
            System.exit(1);
        }

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
*/

    @Override
    public void run() {
        String logFile = "/home/opc/Documents/cursor_projects/tsbechaudit/log/read_thread_" + threadNumber + ".log";
        try {
            logger = new BufferedWriter(new FileWriter(Paths.get(logFile).toFile()));
            conn.setAutoCommit(true); //con select no se modifica el estado de la base de datos, asi que no se necesita commit() y rollback()
            runQueryLoop();
            conn.close();
        } catch (Exception e) {
            System.err.println("Thread " + threadNumber + " error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isDone = true;
            try {
                if (logger != null) logger.close();
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (Exception ignored) {}
        }
    }


    private void runQueryLoop() {
        PreparedStatement prepStmt;
        long start, end;

        String sql = "SELECT id FROM sift1B ORDER BY vector_distance(embedding, ?, EUCLIDEAN) FETCH FIRST 3 ROWS ONLY";
        
        try {
            prepStmt = conn.prepareStatement(sql);
            start = System.currentTimeMillis();

            while (true) {
                int rowCount = 0;
                float[] vector = generarVector(vectorDim);

                Instant startQuery = Instant.now();
                try {
                    prepStmt.setObject(1, vector, OracleType.VECTOR_FLOAT32);
                    ResultSet rs = prepStmt.executeQuery();
                    /*
                    while (rs.next()) {
                        rs.getInt(1); //aca se esta midiendo tambien la lectura de filas (confirmar con Diego si esto se debe medir)
                        rowCount++; //esto tambien aumenta tiempo, ver donde reubicarlo
                    }
                    */
                } catch (Exception e) {
                    System.out.println("Error en select ThreadRead: " + e);
                    rowCount = 0;
                }
                Instant endQuery = Instant.now();

                writeLog("Query," + rowCount + "," + Duration.between(startQuery, endQuery).toMillis());

                end = System.currentTimeMillis();
                duration = (int) (end - start) / 1000;
                if (duration > maxDuration) {
                    prepStmt.close();
                    closeWriteLog();
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error in query execution: " + e.getMessage());
            e.printStackTrace();
        }
    }


/*
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
*/



    private float[] generarVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = (float) ThreadLocalRandom.current().nextDouble();
        }
        return vector;
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

    private void closeWriteLog() {
        try {
            if (logger != null) {
                logger.close();
            }
        } catch (Exception e) {
            System.out.println("Error closing log file: " + e);
            System.exit(1);
        }
    }

/*
    private int getRandomNumInRange(int max) {

        if (1 >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - 1) + 1) + 1;
    }
*/

    public boolean isDone() {
        return isDone;
    }
/*
    public int getCount() {
        return count;
    }
*/
    public int getDuration() {
        return duration;
    }

}
