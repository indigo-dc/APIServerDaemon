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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * Runnable class that controls GridEngineDaemon activities such as:
 * - Update job status values of any submitted task 
 * - Manage job output request
 * - Preserve consistency status of any broken activity
 * - Cleanup done operations
 * This class implements one of the two principal GridEngineDaemon threads
 * together with GridEngineDaemonPolling class
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemonPolling
 */
public class GridEngineDaemonController implements Runnable {
    /*
      GridEngine API Server Database settings
    */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    
    /*
      GridEngineDaemon Controller settings
    */ 
    private boolean geControllerStatus = true;
    private int     geControllerDelay;
    private int     geControllerMaxCommands;
    /*
      GridEngineDaemon config
    */
    GridEngineDaemonConfig gedConfig;
    
    /*
      Thread pool executor
    */
    private ExecutorService gedExecutor = null;
    
    String threadName;
    
    /*
      Logger
    */    
    private static final Logger _log = Logger.getLogger(GridEngineDaemonController.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    /**
     * Instantiate a GridEngineDaemonController allowing to execute further 
     * threads using the given Executor object
     * @param gedExecutor Executor object created by the GridEngineDaemon
     */
    public GridEngineDaemonController(ExecutorService gedExecutor) {
        this.gedExecutor = gedExecutor;
        threadName = Thread.currentThread().getName();
        _log.info("Initializing GridEngine PollingThread");
    }
    
    /**
     * Load GridEngineDaemon configuration settings
     * @param gedConfig GridEngineDaemon configuration object
     */
    public void setConfig(GridEngineDaemonConfig gedConfig) {
        // Save configs
        this.gedConfig=gedConfig;
        // Set configuration values for this class
        this.apisrv_dbhost = gedConfig.getApisrv_dbhost();
        this.apisrv_dbport = gedConfig.getApisrv_dbport();
        this.apisrv_dbuser = gedConfig.getApisrv_dbuser();
        this.apisrv_dbpass = gedConfig.getApisrv_dbpass();
        this.apisrv_dbname = gedConfig.getApisrv_dbname();
        // Load GridEngineDaemon settings
        this.geControllerDelay       = gedConfig.getControllerDelay();
        this.geControllerMaxCommands = gedConfig.getControllerMaxCommands();   
        _log.info(
              "GridEngineDaemon config:"                                   +LS
           +"  [Database]"                                                 +LS
           +"    db_host: '"              +this.apisrv_dbhost          +"'"+LS
           +"    db_port: '"              +this.apisrv_dbport          +"'"+LS
           +"    db_user: '"              +this.apisrv_dbuser          +"'"+LS
           +"    db_pass: '"              +this.apisrv_dbpass          +"'"+LS
           +"    db_name: '"              +this.apisrv_dbname          +"'"+LS
           +"  [Controller config]"                                        +LS
           +"    ControllerDelay      : '"+this.geControllerDelay      +"'"+LS
           +"    ControllerMaxCommands: '"+this.geControllerMaxCommands+"'"+LS);
    }

    
    @Override
    public void run() {
        GridEngineDaemonDB gedDB = null;
        
        _log.info("Starting GridEngine ControllerThread");
        /**
         * GridEngineDaemonController 'run' method loops until geControllerStatus
         * is true
         * Polling loops takes only the following kind of command statuses
         * from the ge_queue:
         *  - PROCESSING: Verify time consistency retrying command if necessary
         *  - SUBMITTED : as above
         *  - RUNNING   : as above
         *  - DONE      : Cleanup allocated space for expired tasks
         * table and then process them with the GrirEngineDaemonProcessCommand
         * The same kind of loop exists in the GridEngineDaemonPolling
         * @see GridEngineDaemonProcessCommand
         * @see GridEngineDaemonPolling
         */        
        while(geControllerStatus) {
            try {
                /*
                  Retrieves commands from DB
                */
                gedDB= new GridEngineDaemonDB(apisrv_dbhost
                                             ,apisrv_dbport
                                             ,apisrv_dbuser
                                             ,apisrv_dbpass
                                             ,apisrv_dbname);
                List<GridEngineDaemonCommand> 
                    commands = gedDB.getControllerCommands(geControllerMaxCommands);
                _log.debug("Received "
                          +commands.size()+"/"+geControllerMaxCommands
                          +" controller commands");
                /*
                  Process retrieved commands
                */
                Iterator<GridEngineDaemonCommand> iterCmds = commands.iterator(); 
                while (iterCmds.hasNext()) {
                    GridEngineDaemonCommand geCommand = iterCmds.next();
                    GridEngineDaemonCheckCommand gedProcCmd =
                        new GridEngineDaemonCheckCommand(
                            geCommand
                           ,gedDB.getConnectionURL());
                    if (gedProcCmd != null) {
                        gedProcCmd.setConfig(gedConfig);
                        gedExecutor.execute(gedProcCmd);
                    }                    
                }
            } catch (Exception e) {
                /* Do something */              
                _log.fatal("Unable to get APIServer commands");
            }
            finally {
               if(gedDB!=null) gedDB.close(); 
            }
            /*
              Wait for next loop
            */
            try {
                Thread.sleep(geControllerDelay);
            } catch(InterruptedException e) { geControllerStatus=false; }
        }
    }
    
    /**
     * Terminate the controller loop
     */
    public void terminate() {
        /*
         Tells to the controller thread to exit from its loop
        */
        geControllerStatus = false;
    }
}
