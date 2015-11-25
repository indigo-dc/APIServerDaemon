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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;


/**
 * Servlet context listener object used to instantiate the APIServerDaemon
 class and initalize the daemon ThreadPool and its polling thread
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see APIServerDaemon
 */
public class APIServerServletListener implements ServletContextListener {
    
    private static final String LS = System.getProperty("line.separator");
    private static final String PS = System.getProperty("file.separator");
    private static final String US = System.getProperty("user.name");
    private static final String VN = System.getProperty("java.vendor");
    private static final String VR = System.getProperty("java.version");
    
    private APIServerDaemon asDaemon = null;
  //private GridEngineDaemonLogger geDaemonLog = null;
    ServletContext  context;
    
    /**
     * Called during servlet context initalization, it instantiate the
     * APIServerDaemon class
     * @param sce Servlet context event object
     * @see APIServerDaemon
     */
    @Override
    public void contextInitialized(ServletContextEvent sce) {        
        
        context = sce.getServletContext();
        String webAppPath = context.getRealPath(PS);
        System.out.println("--- Starting APIServerDaemon ---");        
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
	    DataSource ds = (DataSource)initialContext.lookup("java:/comp/env/jdbc/fgApiServerPool");
            Connection conn = ds.getConnection();
            System.out.println("PERFECT: jdbc/fgApiServerPool was ok");
        } catch (Exception e) {
            System.err.println("WARNING: jdbc/fgApiServerPool failed"+LS+e.toString());
        }
        
        // Register MySQL driver
        APIServerDaemonDB.registerDriver();
        
        // Initializing the daemon
        if (asDaemon == null) {
            asDaemon = new APIServerDaemon();
        }
        asDaemon.startup();
    }

    /**
     * Called destroying servlet context, it instantiate the APIServerDaemon
 class
     * @param sce 
     * @see APIServerDaemon
     */
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if(asDaemon!=null) asDaemon.shutdown();
        
        // Unregister MySQL driver
        APIServerDaemonDB.unregisterDriver();
    }
}