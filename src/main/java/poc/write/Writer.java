package poc.write;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import java.io.File;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Clase principal que maneja las operaciones de escritura en la base de datos.
 * Configura el pool de conexiones y gestiona los threads de escritura.
 */
public class Writer implements Runnable {

    // Variables de instancia
    private int threads;
    private final Properties prop;

    // Variables de configuración
    private String url;
    private String user;
    private String password;
    private String factClassDriver;
    private String ucpPoolName;
    private int ucpPoolSize;

    private int connectionWaitTimeout;
    
    //private static int threadThinkTime = 0;
    private boolean validateConnectionOnBorrow;

    /**
     * Constructor de la clase Writer.
     * Inicializa las propiedades desde el archivo de configuración.
     */
    public Writer() {
        this.prop = new Properties();
        try {
            String PROP_FILE = "/home/opc/Documents/cursor_projects/tsbechaudit/src/main/java/poc/ucp-threads.properties";
            this.prop.load(new FileInputStream(PROP_FILE));
        } catch (Exception e) {
            System.out.println("Error on reading properties file: " + e);
        }
    }

    /**
     * Método principal que se ejecuta cuando el thread inicia.
     * Configura el pool de conexiones y ejecuta los threads de escritura.
     */
    @Override
    public void run() {
        try {
            // Configura el pool de conexiones
            PoolDataSource pds = getPoolDataSource();
            show("Shared Connection pool " + pds.getConnectionPoolName() + " configured");

            // Obtiene el número de threads del archivo de propiedades
            threads = Integer.parseInt(prop.getProperty("app.writerThreads"));
            System.out.println("Starting all " + threads + " writer virtual threads\n");

            cleanLogDirectory();

            // Crea y ejecuta los threads de escritura usando Virtual Threads
            WriteRequest[] wThread;
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                wThread = new WriteRequest[threads];
                System.out.println("wThread.length: " + wThread.length);
                
                for (int i = 0; i < wThread.length; i++) {
                    System.out.println("i: " + i);
                    wThread[i] = new WriteRequest(
                        i,
                        pds.getConnection(),
                        Integer.parseInt(prop.getProperty("app.maxDuration")),
                        Integer.parseInt(prop.getProperty("app.insertBatchSize")),
                        Integer.parseInt(prop.getProperty("vector.dimension"))
                    );
                    executorService.execute(wThread[i]);
                }
            }

            System.out.println(wThread.length + " writer virtual threads started -" + 
                " Operation: " + prop.getProperty("app.operation") + 
                " - Array Size: " + prop.getProperty("app.insertBatchSize") + "\n");
            
            monitor(wThread);

        } catch (Exception e) {
            System.out.println("Error on connect database: " + e);
        }
    }

    /**
     * Monitorea el progreso de los threads de escritura.
     * @param wThread Array de threads de escritura a monitorear
     */
    public void monitor(WriteRequest[] wThread) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int countDone;
            int countInserts;
            int duration;

            while (true) {
                TimeUnit.SECONDS.sleep(10);
                countDone = 0;
                countInserts = 0;
                duration = 0;

                for (WriteRequest writeRequest : wThread) {
                    if (writeRequest.isDone())
                        countDone++;
                }

                if (countDone == threads) {
                    System.out.println("\nFinished all threads");

                    for (WriteRequest writeRequest : wThread) {
                        countInserts += writeRequest.getCount();
                        duration = Math.max(duration, writeRequest.getDuration());
                    }

                    if (countInserts > 0)
                        System.out.format("%16s%10s%7d%14s%6d%n", 
                            sdf.format(new Date().getTime()), 
                            "Writes:", 
                            countInserts, 
                            "Writes/sec:",
                            countInserts/duration);

                    System.exit(0);
                }
            }
        } catch (Exception e) {
            System.out.println("Error on monitor: " + e);
        }
    }

    /**
     * Configura y retorna el pool de conexiones.
     * @return PoolDataSource configurado
     * @throws SQLException si hay error en la configuración
     */
    private PoolDataSource getPoolDataSource() throws SQLException {
        show("\n#################### PoolDataSource Thread Class starts ####################");

        // Lee configuración del archivo de propiedades
        url = prop.getProperty("db.url");
        user = prop.getProperty("db.username");
        password = prop.getProperty("db.password");
        factClassDriver = prop.getProperty("db.datasource");
        validateConnectionOnBorrow = Boolean.parseBoolean(prop.getProperty("validateConnectionOnBorrow","false"));
        connectionWaitTimeout = Integer.parseInt(prop.getProperty("app.connectionWaitTimeout","3"));
        ucpPoolName = prop.getProperty("app.ucp_pool_name");
        ucpPoolSize = Integer.parseInt(prop.getProperty("app.ucp_pool_size"));
        threads = Integer.parseInt(prop.getProperty("app.writerThreads"));

        // Muestra configuración
        System.out.println("############################################################################");
        show("db.url                           : " + url);
        show("db.datasource                    : " + factClassDriver);
        show("Connecting to user               : " + user);
        show("# of writer virtual threads                   : " + threads);
        show("UCP pool size                  : " + ucpPoolSize);
        System.out.println("############################################################################");

        // Configura el pool de conexiones
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();

        // Configura propiedades basicas requeridas
        pds.setURL(url);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setConnectionFactoryClassName(factClassDriver);
        pds.setValidateConnectionOnBorrow(validateConnectionOnBorrow); //validar con el cliente si esta linea es necesaria. Valida si una conexión es buena antes de darla al solicitante. Cuando está activo, mejora la estabilidad pero introduce un pequeño overhead.
        pds.setTimeoutCheckInterval(connectionWaitTimeout);

        // Configura propiedades del pool de conexiones
        pds.setConnectionPoolName(ucpPoolName);
        pds.setInitialPoolSize(5);
        pds.setMinPoolSize(10);
        pds.setMaxPoolSize(ucpPoolSize);

        /*propiedades para mysql 
        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("autoReconnect", "true");
        properties.setProperty("serverTimezone", "UTC");
        pds.setConnectionProperties(properties);
        */

        show("Connection pool: " + ucpPoolName);
        show("Testing borrow connections from and return connections to the connection pool: ");

        // Prueba la conexión
        try (Connection conn1 = pds.getConnection()) {
            showPoolStatistics("After checkin", pds);
            conn1.close();
        } catch (SQLException exc) {
            showError("1st checkout", exc);
        }
        showPoolStatistics("After checkout", pds);
        
        return pds;
    }

    /**
     * Muestra un mensaje de error.
     * @param msg Mensaje descriptivo
     * @param exc Excepción ocurrida
     */
    static void showError(String msg, Throwable exc) {
        System.err.println(msg + " hit error: " + exc.getMessage());
    }

    /**
     * Muestra un mensaje informativo.
     * @param msg Mensaje a mostrar
     */
    static void show(String msg) {
        System.out.println(msg);
    }

    /**
     * Muestra estadísticas del pool de conexiones.
     * @param prompt Prefijo del mensaje
     * @param pds Pool de conexiones
     * @throws SQLException si hay error al obtener estadísticas
     */
    private static void showPoolStatistics(String prompt, PoolDataSource pds) throws SQLException {
        show(prompt + " -");
        show("  Available connections: " + pds.getAvailableConnectionsCount());
        show("  Borrowed connections: " + pds.getBorrowedConnectionsCount());
    }

    private void cleanLogDirectory() {
        File logDir = new File("/home/opc/Documents/cursor_projects/tsbechaudit/log");
        File[] oldLogs = logDir.listFiles((dir, name) -> name.startsWith("read_thread_") && name.endsWith(".log"));
    
        if (oldLogs != null) {
            for (File log : oldLogs) {
                if (log.delete()) {
                    System.out.println("Deleted old log: " + log.getName());
                }
            }
        }
    }
}