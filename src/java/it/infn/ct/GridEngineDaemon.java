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
 * GridEngineDaemon class instantiates the threadPool daemon and its main 
 * polling thread
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineDaemon {    
    /*
      GridEngine API Server Database settings
    */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    
    private int                        gedMaxThreads   =  100;
    private int                        gedCloseTimeout =   20;
    private ExecutorService            gedExecutor     = null;
    private GridEngineDaemonPolling    gedPolling;
    private GridEngineDaemonController gedController;
    
    /*
      GridEngineDaemon configuration
    */
    GridEngineDaemonConfig gedConfig;
    
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(GridEngineDaemon.class.getName());
    
    private static final String LS = System.getProperty("line.separator");        
    
    /**
     * Class constructor, called by the ServletListener upon startup
     */
    public GridEngineDaemon() {
        // Load static configuration
        gedConfig = new GridEngineDaemonConfig();
        
        // Set configuration values for this class
        this.apisrv_dbhost = gedConfig.getApisrv_dbhost();
        this.apisrv_dbport = gedConfig.getApisrv_dbport();
        this.apisrv_dbuser = gedConfig.getApisrv_dbuser();
        this.apisrv_dbpass = gedConfig.getApisrv_dbpass();
        this.apisrv_dbname = gedConfig.getApisrv_dbname();
        // Load GridEngineDaemon settings
        this.gedMaxThreads   = gedConfig.getMaxThreads();
        this.gedCloseTimeout = gedConfig.getCloseTimeout();   
        _log.debug(
                  "GridEngineDaemon config:"                        +LS
                 +"  [Database]"                                    +LS
                 +"    db_host: '"+ this.apisrv_dbhost          +"'"+LS
                 +"    db_port: '"+ this.apisrv_dbport          +"'"+LS
                 +"    db_user: '"+ this.apisrv_dbuser          +"'"+LS
                 +"    db_pass: '"+ this.apisrv_dbpass          +"'"+LS
                 +"    db_name: '"+ this.apisrv_dbname          +"'"+LS
                 +"  [ThreadPool config]"                           +LS
                 +"    gedMaxThreads  : '"+this.gedMaxThreads   +"'"+LS
                 +"    gedCloseTimeout: '" +this.gedCloseTimeout+"'"+LS
                 );
    }
    
    /**
     * Initialize the GridEngine daemon Threadpool and its main polling loop
     */
    void startup() {
        _log.info("Initializing GridEngine Daemon");        
        /*
         Before to start the daemon, verify that all conditions are satisfied
        */
        // GridEngineDB (who polls otherwise)
        // SAGA stuff (how shoould I execute)
        /*
         Initialize the thread pool
        */
        gedExecutor = Executors.newFixedThreadPool(gedMaxThreads);
        /*
         The first thread in the Pool is the polling thread
        */
        gedPolling = new GridEngineDaemonPolling(gedExecutor);
        gedPolling.setConfig(gedConfig);
        gedExecutor.execute(gedPolling);
        /*
         The second thread in the Pool is the controller thread
        */        
        gedController = new GridEngineDaemonController(gedExecutor);
        gedController.setConfig(gedConfig);
        gedExecutor.execute(gedController);
        _log.info("Executed polling thread");
    }

    /**
     * Terminate the thread pool and its threads
     */
    void shutdown() {
        gedPolling.terminate(); _log.info("Terminated polling thread");
        try {
            gedExecutor.shutdown();
            gedExecutor.awaitTermination(gedCloseTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            _log.info("GridEngine Daemon polling termination cancelled");
        } finally {
            if(!gedExecutor.isTerminated()) {
               _log.warn("Thread pool closure not finished");
            }
            gedExecutor.shutdownNow();
            _log.warn("GridEngine Daemon forcing termination");
        }
        _log.info("GridEngine Daemon terminated");        
    }

}

    
    
    
    
