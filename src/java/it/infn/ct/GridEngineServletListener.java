/**************************************************************************
Copyright (c) 2011:
Istituto Nazionale di Fisica Nucleare (INFN), Italy
Consorzio COMETA (COMETA), Italy

See http://www.infn.it and and http://www.consorzio-cometa.it for details on
the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
****************************************************************************/

package it.infn.ct;

import java.io.File;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;


/**
 * Servlet context listener object used to instantiate the GridEngineDaemon
 * class and initalize the daemon ThreadPool and its polling thread
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemon
 */
public class GridEngineServletListener implements ServletContextListener {
    
    private static final String LS = System.getProperty("line.separator");
    private static final String PS = System.getProperty("file.separator");
    private static final String US = System.getProperty("user.name");
    private static final String VN = System.getProperty("java.vendor");
    private static final String VR = System.getProperty("java.version");
    
    private GridEngineDaemon geDaemon = null;
  //private GridEngineDaemonLogger geDaemonLog = null;
    ServletContext  context;
    
    /**
     * Called during servlet context initalization, it instantiate the
     * GridEngineDaemon class
     * @param sce Servlet context event object
     * @see GridEngineDaemon
     */
    @Override
    public void contextInitialized(ServletContextEvent sce) {        
        
        context = sce.getServletContext();
        String webAppPath = context.getRealPath(PS);
        System.out.println("--- Starting GridEngineDaemon ---");        
        System.out.println("Java vendor : '" + VN +"'");
        System.out.println("Java vertion: '" + VR +"'");
        System.out.println("Running as  : '" + US +"' username");
        System.out.println("Servlet path: '" + webAppPath +"'");
        
        // Initialize log4j logging
        String log4jPropPath = webAppPath + "WEB-INF"+PS+"log4j.properties";
        File log4PropFile = new File(log4jPropPath);
        if (log4PropFile.exists()) {
            System.out.println("Initializing log4j with: " + log4jPropPath);
            PropertyConfigurator.configure(log4jPropPath);            
        } else {
            System.err.println("WARNING: '" + log4jPropPath + "' file not found, so initializing log4j with BasicConfigurator");
                BasicConfigurator.configure();
        }
        
        // Make a test with jdbc/geApiServerPool
        try {
            Context initContext = new InitialContext();
            //Context envContext  = (Context)initContext.lookup("java:/comp/env");                        
            //DataSource ds = (DataSource)envContext.lookup("jdbc/geApiServerPool");
            
            Context initialContext = new InitialContext();
	    DataSource ds = (DataSource)initialContext.lookup("java:/comp/env/jdbc/geApiServerPool");
            Connection conn = ds.getConnection();
            System.out.println("PERFECT: jdbc/geApiServerPool was ok");
        } catch (Exception e) {
            System.err.println("WARNING: jdbc/geApiServerPool failed");
        }
        
        // Register MySQL driver
        GridEngineDaemonDB.registerDriver();
        
        // Initializing the daemon
        if (geDaemon == null) {
            geDaemon = new GridEngineDaemon();
        }
        geDaemon.startup();
    }

    /**
     * Called destroying servlet context, it instantiate the GridEngineDaemon
     * class
     * @param sce 
     * @see GridEngineDaemon
     */
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if(geDaemon!=null) geDaemon.shutdown();
        
        // Unregister MySQL driver
        GridEngineDaemonDB.unregisterDriver();
    }
}