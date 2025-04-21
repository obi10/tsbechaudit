package poc.write;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.*;

/**
 * Clase que maneja las operaciones de escritura en la base de datos.
 * Cada instancia representa un thread de escritura que puede realizar
 * inserciones individuales o en lote.
 */
public class WriteRequest implements Runnable {

    // Variables de instancia
    private final int threadNumber;
    private final Connection conn;
    private final int maxDuration;
    private final int batchSize;
    private final int vectorDim;

    private BufferedWriter logger;
    private int count = 0;
    private int duration = 0;
    private boolean isDone = false;

    
    /**
     * Constructor de WriteRequest
     * @param threadNumber Número identificador del thread
     * @param conn Conexión a la base de datos
     * @param maxDurationSeconds Duración máxima de la prueba en segundos
     * @param batchSize Tamaño del lote para inserciones
     * @param vectorDim Dimensión del vector
     */
    public WriteRequest(int threadNumber, Connection conn, int maxDurationSeconds, int batchSize, int vectorDim) {
        this.threadNumber = threadNumber;
        this.conn = conn;
        this.maxDuration = maxDurationSeconds;
        this.batchSize = batchSize;
        this.vectorDim = vectorDim;
    }

    /**
     * Método principal que se ejecuta cuando el thread inicia.
     * Decide si ejecutar inserciones individuales o en lote basado en el batchSize.
     */
    @Override
    public void run() {
        String logFile = "log/write_thread_" + threadNumber + ".log";

        try {
            logger = new BufferedWriter(new FileWriter(Paths.get(logFile).toFile()));
            conn.setAutoCommit(false);
            if (batchSize > 1) {
                runBatchInsert(logger);
            } else {
                runSingleInsert(logger);
            }
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

    /**
     * Realiza inserciones individuales, unitarias en la base de datos. Insert sin batch.
     * Continúa hasta que se alcanza el maxDuration.
     */
    private void runSingleInsert(BufferedWriter logger) throws SQLException, IOException {
        PreparedStatement prepStmt;
        long start, end;
        count = 0;

        String insert = "INSERT INTO embeddings (item_id, embedding) VALUES (?, VECTOR(?, ?))";

        try {
            prepStmt = conn.prepareStatement(insert);
            start = System.currentTimeMillis(); //medir el tiempo total de ejecucion

            while (true) {
                int numUPd = 0; //buena practica si ocurre una excepcion

                String itemId = "item_" + count;
                float[] vector = generarVector(vectorDim);

                Instant startInsert = Instant.now(); //empieza a medir tiempo de insert unitario + commit
                try {
                    prepStmt.setString(1, itemId);
                    prepStmt.setObject(2, vector);
                    prepStmt.setInt(3, vectorDim);
                    numUPd = prepStmt.executeUpdate();
                    conn.commit();
                } catch (Exception e) {
                    System.out.println("Error en insert ThreadWrite: " + e);
                    conn.rollback(); //si ocurre una excepcion, revertimos la insert, para evitar inconsistencias
                    numUPd = 0;
                }
                Instant endInsert = Instant.now(); //termina de medir tiempo de insert unitario + commit

                //se escribe en el archivo log, el tiempo que toma un insert unitario + commit
                writeLog("Ins"+batchSize+"," + numUPd + "," + Duration.between(startInsert, endInsert).toMillis());

                end = System.currentTimeMillis();
                duration = (int) (end - start) / 1000; //calcular duracion total en segundos

                if (duration > maxDuration) { //verificar si ha excedido el tiempo maximo de duracion
                    prepStmt.close();
                    closeWriteLog();
                    break; //detener el ciclo si se supera el tiempo maximo
                }

                count++;  //incrementar el contador de inserts
            }
        } catch (Exception e) {
            System.out.println("Error en insert ThreadWrite: " + e);
        }
    }

    /**
     * Realiza inserciones en batch en la base de datos.
     * batchSize es el tamaño del lote de inserciones
     */
    public void runBatchInsert(BufferedWriter logger) throws SQLException, IOException {
        PreparedStatement prepStmt;
        long start, end;
        count = 0;

        String insert = "INSERT INTO embeddings (item_id, embedding) VALUES (?, VECTOR(?, ?))";

        try {
            prepStmt = conn.prepareStatement(insert);
            start = System.currentTimeMillis();
            
            while (true) {
                for (int i = 0; i < batchSize; i++) {
                    String itemId = "item_" + (count + i);
                    float[] vector = generarVector(vectorDim);
                    prepStmt.setString(1, itemId);
                    prepStmt.setObject(2, vector);
                    prepStmt.setInt(3, vectorDim);
                    prepStmt.addBatch();
                }
                
                Instant startBatch = Instant.now(); //empieza a medir tiempo de insert en batch + commit
                try {
                    int[] rows = prepStmt.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    System.out.println("Error en batchUpdate ThreadWrite: " + e);
                    conn.rollback();
                }
                Instant endBatch = Instant.now(); //termina en medir tiempo de insert en batch + commit

                writeLog("BatchIns1," + batchSize + "," + Duration.between(startBatch, endBatch).toMillis());

                count += batchSize;

                end = System.currentTimeMillis();
                duration = (int) (end - start) / 1000;

                if (duration > maxDuration) {
                    prepStmt.close();
                    closeWriteLog();
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Error en batch insert method - ThreadWrite: " + e);
        }
    }

    /**
     * Genera un vector aleatorio de dimensión dim.
     * @return vector aleatorio
     * Se usa ThreadLocalRandom para que cada virtual thread tenga su propio generador de numeros aleatorios
     */
    private static float[] generarVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = (float) ThreadLocalRandom.current().nextDouble();
        }
        return vector;
    }


    /**
     * Escribe un mensaje en el archivo de log.
     * @param str Mensaje a escribir
     */
    public void writeLog(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            logger.write(sdf.format(new Date().getTime()) + "," + str + "\n");
        } catch (Exception e) {
            System.out.println("Error on writing to the log file: " + e);
            System.exit(1);
        }
    }

    /**
     * Cierra el writer del archivo de log.
     */
    public void closeWriteLog() {
        try {
            if (logger != null) {
                logger.close();
            }
        } catch (Exception e) {
            System.out.println("Error closing log file: " + e);
            System.exit(1);
        }
    }

    // Métodos getter
    public boolean isDone() {
        return isDone;
    }

    public int getCount() {
        return count;
    }

    public int getDuration() {
        return duration;
    }


/*
    public JsonNode getAuditTrail() {
        JsonNode rootNode;
        try {
            byte[] jsonData = Files.readAllBytes(Paths.get("/Users/junicode/Documents/developer_junicode/cursor_projects/tsbechaudit/src/main/java/poc/audit-trail.json"));
            ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            rootNode = objectMapper.readTree(jsonData);
            JsonNode nameNode = rootNode.path("audit_name");
            System.out.println("audit_name = "+nameNode.asText());
            System.out.println("client_timestamp = "+getTS());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rootNode;
    }

    public Timestamp getTS() {
        Timestamp iniTS = Timestamp.valueOf("2014-01-01 00:00:00");
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
*/

}
