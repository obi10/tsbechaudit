package poc.read;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

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

public class Reader implements Runnable{
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
    private boolean validateConnectionOnBorrow;

    public Reader() {
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
     * Configura el pool de conexiones y ejecuta los threads de lectura.
     */
    @Override
    public void run() {
        try {
            try {
                // Configura el pool de conexiones
                PoolDataSource pds = getPoolDataSource();
                show("Shared Connection pool " + pds.getConnectionPoolName() + " configured");

                // Obtiene el número de threads del archivo de propiedades
                threads = Integer.parseInt(prop.getProperty("app.readerThreads"));
                System.out.println("Starting all " + threads + " reader virtual threads\n");

                // Crea y ejecuta los threads de lectura usando Virtual Threads
                ReadRequest[] rThread;
                try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                    rThread = new ReadRequest[threads];
                    System.out.println("rThread.length: " + rThread.length);
                    
                    for (int i = 0; i < rThread.length; i++) {
                        System.out.println("i: " + i);
                        rThread[i] = new ReadRequest(
                            i,
                            pds.getConnection(),
                            Integer.parseInt(prop.getProperty("app.maxDuration")),
                            Integer.parseInt(prop.getProperty("vector.dimension"))
                        );
                        executorService.execute(rThread[i]);
                    }
                }

                System.out.println(rThread.length + " reader virtual threads started -" + 
                    " Operation: " + prop.getProperty("app.operation") + 
                    " - Array Size: " + prop.getProperty("app.insertBatchSize") + "\n"); //despues se analizara si se elimina Array Size
                
                monitor(rThread);

            } catch (SQLException exc) {
                showError("1st checkout - thread", exc);
            }
        } catch (Exception e) {
            System.out.println("Error on connect database: " + e);
        }

    }

    public void monitor(ReadRequest[] rThread) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int countDone;
            int duration;

            while (true) {
                TimeUnit.SECONDS.sleep(10);
                countDone = 0;
                duration = 0;

                for (ReadRequest readRequest : rThread) {
                    if (readRequest.isDone())
                        countDone++;
                }

                if (countDone == threads) {
                    System.out.println("\nFinished all threads");
                    for (ReadRequest readRequest : rThread) {
                        duration = Math.max(duration, readRequest.getDuration());
                    }
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            System.out.println("Error on monitor: " + e);
        }
    }


/*
    public void monitor(ReadRequest[] rThread) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int countDone;
            int countQuery;
            int duration;
            show("monitor- while");
            while (true) {
                // sleep
                TimeUnit.SECONDS.sleep(10);
                show("10 seconds- thread");

                countDone  = 0;
                duration   = 0;
                countQuery = 0;

                for (ReadRequest readRequest : rThread) {
                    if (readRequest.isDone())
                        countDone++;
                }

                if (countDone == threads) {
                    System.out.println("\nFinished all read threads");
                    System.exit(0);
                }

                for (ReadRequest readRequest : rThread) {
                    countQuery += readRequest.getCount();
                    duration = Math.max(duration, readRequest.getDuration());
                }

                System.out.format("%16s%10s%7d%14s%6d%n", sdf.format(new Date().getTime()), "Reads:", countQuery, "Reads/sec:",countQuery/duration);
            }

        } catch (Exception e) {
            System.out.println("Error on monitor: " + e);
        }
    }
*/



    private PoolDataSource getPoolDataSource() throws SQLException {
        show("\n#################### PoolDataSource Thread Class starts ####################");

        // get properties file configuration
        url = prop.getProperty("db.url");
        user = prop.getProperty("db.username");
        password = prop.getProperty("db.password");
        factClassDriver = prop.getProperty("db.datasource");
        validateConnectionOnBorrow = Boolean.parseBoolean(prop.getProperty("validateConnectionOnBorrow","flase"));
        connectionWaitTimeout = Integer.parseInt(prop.getProperty("app.connectionWaitTimeout","3"));
        ucpPoolName = prop.getProperty("app.ucp_pool_name");
        ucpPoolSize = Integer.parseInt(prop.getProperty("app.ucp_pool_size"));
        threads = Integer.parseInt(prop.getProperty("app.readerThreads"));
        //threadThinkTime = Integer.parseInt(prop.getProperty("app_thread_think_time","10"));
        

        System.out.println("############################################################################");
        show("db.url                           : "+url);
        show("db.datasource                    : "+factClassDriver);
        show("Connecting to user               : "+user);
        show("# of reader virtual Threads                   : "+threads);
        show("UCP pool size                  : "+ucpPoolSize);
        System.out.println("############################################################################");


        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();

        pds.setURL(url);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setConnectionFactoryClassName(factClassDriver);
        pds.setValidateConnectionOnBorrow(validateConnectionOnBorrow);
        pds.setTimeoutCheckInterval(connectionWaitTimeout);
        //pds.setConnectionWaitTimeout(Duration.ofMillis(connectionWaitTimeout));

        pds.setConnectionPoolName(ucpPoolName);
        pds.setInitialPoolSize(5);
        pds.setMinPoolSize(10);
        pds.setMaxPoolSize(ucpPoolSize);

        //Log WARNING
        //System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");
        //System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");

        show("Connection pool: " + ucpPoolName);
        show("Testing borrow connections from and return connections to the connection pool: ");

        try (Connection conn1 = pds.getConnection()) {
            showPoolStatistics("After checkin", pds);
            conn1.close();
        } catch (SQLException exc) {
            showError("1st checkout", exc);
        }
        showPoolStatistics("After checkout", pds);
        return pds;
    }

    static void showError(String msg, Throwable exc) {
        System.err.println(msg + " hit error: " + exc.getMessage());
    }

    static void show(String msg) {
        System.out.println(msg);
    }

    private static void showPoolStatistics(String prompt, PoolDataSource pds)
            throws SQLException {
        show(prompt + " -");
        show("  Available connections: " + pds.getAvailableConnectionsCount());
        show("  Borrowed connections: " + pds.getBorrowedConnectionsCount());
    }

}
