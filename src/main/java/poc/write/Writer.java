package poc.write;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Writer implements Runnable {

    private int threads;
    private final Properties prop;
    final static String DEFAULT_URL = "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=gf45e1dc1a8feaa_im02atpdb_tp.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))";
    final static String DEFAULT_USER = "ADMIN";
    final static String DEFAULT_PASSWORD = "Us3rAdm1n2024";
    final static String CONN_FACTORY_CLASS = "oracle.jdbc.pool.OracleDataSource";
    final static String UCP_POOL_NAME = "AUDIT-UCP-WRITER-POOL";

    // You must provide non-default values for ALL 3 to execute the program
    static String url = DEFAULT_URL;
    static String user = DEFAULT_USER;
    static String password = DEFAULT_PASSWORD;
    static String factclassdriver = CONN_FACTORY_CLASS;

    // Number of concurrent threads running in the application
    // UCP is tuned to have MAX and MIN limit set to this
    // How often should the thread print statistics.   Time in milliseconds
    static int connectionWaitTimeout = 3; // seconds
    static int ucpPoolSize = 0;
    static int threadThinkTime = 0;

    // Do not require validateConnectionOnBorrow if patch 31112088 applied
    static boolean validateConnectionOnBorrow = false;
    //static boolean cpuIntensive = false;
    public Writer() {
        this.prop = new Properties();
        try {
            String PROP_FILE = "/home/opc/Documents/tsbechaudit/src/main/java/poc/ucp-threads.properties";
            this.prop.load(new FileInputStream(PROP_FILE));
        } catch (Exception e) {
            System.out.println("Error on reading properties file: " + e);
        }
    }

    public void run()  {
        try {
            /*
             * Step 1 - creates a pool-enabled data source instance
             */
            PoolDataSource pds = getPoolDataSource();
            show("Shared Connection pool " +  pds.getConnectionPoolName()+" configured");

            /*
             * Step 2 - get app writer threads from properties file
             */
            threads = Integer.parseInt(prop.getProperty("app.writerThreads"));
            System.out.println("Starting all " + threads + " writer threads\n");

            WriteRequest[] wThread;
            try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
                wThread = new WriteRequest[threads];

                for (int i = 0; i < wThread.length; i++) {
                    wThread[i] = new WriteRequest(i,
                            pds.getConnection(),
                            Integer.parseInt(prop.getProperty("app.maxDuration")),
                            Integer.parseInt(prop.getProperty("app.insertBatchSize")));

                    executorService.execute(wThread[i]);
                }
            }

            System.out.println(wThread.length + " writer threads started -" + " Operation: " +  prop.getProperty("app.operation") + " - Array Size: " + prop.getProperty("app.insertBatchSize") + "\n");
            monitor(wThread);

        } catch (Exception e) {
            System.out.println("Error on connect database: " + e);
        }
    }


    public void monitor(WriteRequest[] wThread) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int countDone;
            int countInserts;
            int duration;

            while (true) {
                // sleep
                TimeUnit.SECONDS.sleep(10);
                countDone = 0;
                countInserts = 0;
                duration  = 0;

                for (WriteRequest writeRequest : wThread) {
                    if (writeRequest.isDone())
                        countDone++;
                }

                if (countDone == threads) {
                    System.out.println("\nFinished all threads");
                    System.exit(0);
                }

                for (WriteRequest writeRequest : wThread) {
                    countInserts += writeRequest.getCount();
                    duration = Math.max(duration, writeRequest.getDuration());
                }

                if (countInserts > 0 )
                    System.out.format("%16s%10s%7d%14s%6d%n", sdf.format(new Date().getTime()), "Writes:", countInserts, "Writes/sec:",countInserts/duration);
            }

        } catch (Exception e) {
            System.out.println("Error on monitor: " + e);
        }
    }

    private PoolDataSource getPoolDataSource() throws SQLException {
        show("\n#################### PoolDataSource Thread Class starts ####################");

        // get properties file configuration
        url = prop.getProperty("db.url");
        user = prop.getProperty("db.username");
        password = prop.getProperty("db.password");
        factclassdriver = prop.getProperty("db.datasource");
        ucpPoolSize = Integer.parseInt(prop.getProperty("app.ucp_pool_size"));
        //threadThinkTime = Integer.parseInt(prop.getProperty("app_thread_think_time","10"));
        validateConnectionOnBorrow = Boolean.parseBoolean(prop.getProperty("validateConnectionOnBorrow","true"));
        //connectionWaitTimeout = Integer.parseInt(prop.getProperty("app.connectionWaitTimeout","3"));
        threads = Integer.parseInt(prop.getProperty("app.readerThreads"));

        System.out.println("############################################################################");
        show("db.url                           : "+url);
        show("db.datasource                    : "+factclassdriver);
        show("Connecting to user               : "+user);
        show("  # of Threads                   : "+threads);
        show("  UCP pool size                  : "+ucpPoolSize);
        System.out.println("############################################################################");

        /*
         * Step 1 - create a pool datasource
         */

        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();

        /*
         * Step 2 - configures pool properties for establishing connections.
         * These include required and optional properties.
         */

        /* Required pool properties */
        // UCP uses a connection factory to create physical connections.
        // This is typically a JDBC driver javax.sql.DataSource or
        // java.sql.Driver implementation class.
        pds.setURL(url);
        pds.setUser(user);
        pds.setPassword(password);
        pds.setConnectionFactoryClassName(factclassdriver);
        pds.setValidateConnectionOnBorrow(validateConnectionOnBorrow);

        /* Optional pool properties */

        // Pool name should be unique within the same JVM instance.
        // It is useful for administrative tasks, such as starting,
        // stopping, refreshing a pool. Setting a pool name is optional
        // but recommended. If user does not set a pool name, UCP will
        // automatically generate one.
        pds.setConnectionPoolName(UCP_POOL_NAME);

        // The default is 0.
        pds.setInitialPoolSize(5);

        // The default is 0.
        pds.setMinPoolSize(10);

        // The default is Integer.MAX_VALUE.
        pds.setMaxPoolSize(ucpPoolSize);

        // Optional properties
        /* Optional pool properties */
        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("autoReconnect", "true");
        properties.setProperty("serverTimezone", "UTC");
        pds.setConnectionProperties(properties);

        //Log WARNING
        //LSystem.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");
        //LSystem.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");

        show("Connection pool - ATP: " + UCP_POOL_NAME);
        show("Testing borrow connections from and return connections to" +
                " the connection pool -ATP: ");
        /*
         * Step 3 - borrow connections from and return connections to
         *          the connection pool.
         */

        // Borrow a connection from UCP. The connection object is a proxy
        // of a physical connection. The physical connection is returned
        // to the pool when Connection.close() is called on the proxy.

        show("Step 4 - Test connection");

        try (Connection conn1 = pds.getConnection()) {
            /*
             * Step 4 - Test connection
             */

            //conn1.setAutoCommit(true);
            show("Step 5 - showPoolStatistics");
            showPoolStatistics("After checkin", pds);
            conn1.close();
        } catch (SQLException exc) {
            showError(exc);
        }
        showPoolStatistics("After checkout", pds);
        return pds;
    }

    static void showError(Throwable exc) {
        System.err.println("1st checkout" + " hit error: " + exc.getMessage());
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