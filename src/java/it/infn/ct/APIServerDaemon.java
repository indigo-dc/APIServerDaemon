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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * APIServerDaemon class instantiates the threadPool daemon and its main 
 polling thread
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class APIServerDaemon {    
    /*
      API Server Database settings
    */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    private String apisrv_dbver;
    
    private int                       asdMaxThreads   =  100;
    private int                       asdCloseTimeout =   20;
    private ExecutorService           asdExecutor     = null;
    private APIServerDaemonPolling    asdPolling;
    private APIServerDaemonController asdController;
    
    /*
      APIServerDaemon configuration
    */
    APIServerDaemonConfig asdConfig;
    
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(APIServerDaemon.class.getName());
    
    private static final String LS = System.getProperty("line.separator");        
    
    /**
     * Class constructor, called by the ServletListener upon startup
     */
    public APIServerDaemon() {
        // Load static configuration
        _log.debug("Loading preferences for APIServerDaemon");
        asdConfig = new APIServerDaemonConfig(true);
        
        // Set configuration values for this class
        this.apisrv_dbhost = asdConfig.getApisrv_dbhost();
        this.apisrv_dbport = asdConfig.getApisrv_dbport();
        this.apisrv_dbuser = asdConfig.getApisrv_dbuser();
        this.apisrv_dbpass = asdConfig.getApisrv_dbpass();
        this.apisrv_dbname = asdConfig.getApisrv_dbname();
        this.apisrv_dbver  = asdConfig.getApisrv_dbver();
        // Load APIServerDaemon settings
        this.asdMaxThreads   = asdConfig.getMaxThreads();
        this.asdCloseTimeout = asdConfig.getCloseTimeout();   
        _log.debug("API Server daemon config:"                      +LS
                 +"  [Database]"                                    +LS
                 +"    db_host: '"+ this.apisrv_dbhost          +"'"+LS
                 +"    db_port: '"+ this.apisrv_dbport          +"'"+LS
                 +"    db_user: '"+ this.apisrv_dbuser          +"'"+LS
                 +"    db_pass: '"+ this.apisrv_dbpass          +"'"+LS
                 +"    db_name: '"+ this.apisrv_dbname          +"'"+LS
                 +"    db_ver : '"+ this.apisrv_dbver           +"'"+LS
                 +"  [ThreadPool config]"                           +LS
                 +"    gedMaxThreads  : '"+this.asdMaxThreads   +"'"+LS
                 +"    gedCloseTimeout: '" +this.asdCloseTimeout+"'"+LS
                 );
    }
    
    /**
     * Retrieve the database version
     * @return 
     */
    private String getDBVer() {
        APIServerDaemonDB asdDB = new APIServerDaemonDB( apisrv_dbhost
                                                        ,apisrv_dbport
                                                        ,apisrv_dbuser
                                                        ,apisrv_dbpass
                                                        ,apisrv_dbname);                                   
        return asdDB.getDBVer();
    }
    
    /**
     * Initialize the APIServer daemon Threadpool and its main polling loop
     */
    void startup() {
        _log.info("Initializing APIServer Daemon");        
        /*
         Before to start the daemon, verify that all conditions are satisfied
        */
        // Verify DB version
        String dbVer=getDBVer();
        if(dbVer.equals(apisrv_dbver)) {            
            // SAGA stuff (how shoould I execute)
            /*
             Initialize the thread pool
            */
            asdExecutor = Executors.newFixedThreadPool(asdMaxThreads);
            /*
             The first thread in the Pool is the polling thread
            */
            asdPolling = new APIServerDaemonPolling(asdExecutor);
            asdPolling.setConfig(asdConfig);
            asdExecutor.execute(asdPolling);
            /*
             The second thread in the Pool is the controller thread
            */        
            asdController = new APIServerDaemonController(asdExecutor);
            asdController.setConfig(asdConfig);
            asdExecutor.execute(asdController);
            _log.info("Executed polling thread");
        } else {
            _log.error("Current database version '"+dbVer+"' is not compatible with requested version '"+apisrv_dbver+"; the APIServerDaemon did not start!");
        }
    }

    /**
     * Terminate the thread pool and its threads
     */
    void shutdown() {
        asdPolling.terminate(); _log.info("Terminated polling thread");
        try {
            asdExecutor.shutdown();
            asdExecutor.awaitTermination(asdCloseTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            _log.info("APIServer Daemon polling termination cancelled");
        } finally {
            if(!asdExecutor.isTerminated()) {
               _log.warn("Thread pool closure not finished");
            }
            asdExecutor.shutdownNow();
            _log.warn("APIServer Daemon forcing termination");
        }
        _log.info("APIServer Daemon terminated");        
    }        
}

    
    
    
    
