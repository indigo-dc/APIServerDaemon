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

//import java.io.BufferedWriter;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

/**
 * This is the Runnable class that implements the polling thread
 * This class implements one of the two principal GridEngineDaemon threads
 * together with GridEngineDaemonPolling class 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemonController
 */
class GridEngineDaemonPolling implements Runnable {
    /*
      GridEngine API Server Database settings
    */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    /*
      GridEngineDaemon Polling settings
    */ 
    private boolean gePollingStatus = true;
    private int     gePollingDelay;
    private int     gePollingMaxCommands;
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
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    /**
     * Constructor receiving the threadpool executor object allowing this
     * class to submit further threads
     * @param gedExecutor The executor object instantiated from GridEngineDaemon
     * class
     * @see GridEngineDaemon
     */
    public GridEngineDaemonPolling(ExecutorService gedExecutor) {
        this.gedExecutor = gedExecutor;
        threadName = Thread.currentThread().getName();
        _log.info("Initializing GridEngine PollingThread");
    }
    
    /**
     * Keeps updated a file containing the polling loop state/health (test)
     * (disabled: this function has been used as preliminary thread tests)
    private void writeState(int loopCount) {
        Writer writer = null;        
        try {                
                writer = new BufferedWriter(
                         new OutputStreamWriter(
                         new FileOutputStream("/tmp/gedaemon.txt"), "utf-8"));
                writer.write("GridEngineDaemon runs ("+loopCount+")");
        } catch (IOException ex) { gePollingStatus=false; }
        finally {
            try { if(writer!=null) writer.close(); } 
            catch (Exception ex) { gePollingStatus=false; }
        }
    }
    */
    
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
        this.gePollingDelay       = gedConfig.getPollingDelay();
        this.gePollingMaxCommands = gedConfig.getPollingMaxCommands();   
        _log.info("GridEngineDaemon config:"                                 +LS
                 +"  [Database]"                                             +LS
                 +"    db_host: '"             +this.apisrv_dbhost       +"'"+LS
                 +"    db_port: '"             +this.apisrv_dbport       +"'"+LS
                 +"    db_user: '"             +this.apisrv_dbuser       +"'"+LS
                 +"    db_pass: '"             +this.apisrv_dbpass       +"'"+LS
                 +"    db_name: '"             +this.apisrv_dbname       +"'"+LS
                 +"  [Polling config]"                                       +LS
                 +"    gePollingDelay  : '"    +this.gePollingDelay      +"'"+LS
                 +"    gePollingMaxCommands: '"+this.gePollingMaxCommands+"'"+LS
                 );
    }

    /**
     * GridEngineDaemonPolling 'run' method loops untill gePollingStatus is true
     * Polling loops takes only WAITING status records from the ge_queue
     * table and then process them with the GrirEngineDaemonProcessCommand
     * The same kind of loop exists in the GridEngineDaemonController
     * @see GridEngineDaemonProcessCommand
     * @see GridEngineDaemonController
     */    
    @Override
    public void run() {
        GridEngineDaemonDB gedDB = null;
        
        _log.info("Starting GridEngine PollingThread");
        /*
          PollingThread main loop; it gets available commands from queue
        */        
        while(gePollingStatus) {
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
                    commands = gedDB.getQueuedCommands(gePollingMaxCommands);
                _log.info("Received "
                         +commands.size()+"/"+gePollingMaxCommands
                         +" waiting commands");
                /*
                  Process retrieved commands
                */
                Iterator<GridEngineDaemonCommand> iterCmds = commands.iterator(); 
                while (iterCmds.hasNext()) {
                    GridEngineDaemonCommand geCommand = iterCmds.next();
                    GridEngineDaemonProcessCommand gedProcCmd =
                        new GridEngineDaemonProcessCommand(
                            geCommand
                           ,gedDB.getConnectionURL());
                    if (gedProcCmd != null) {
                        gedProcCmd.setConfig(gedConfig);
                        gedExecutor.execute(gedProcCmd);
                    }
                }
            } catch (Exception e) {
                /* Do something */
                _log.severe("Unable to get APIServer commands");
            }
            finally {
               if(gedDB!=null) gedDB.close(); 
            }
            /*
              Wait for next loop
            */
            try {
                Thread.sleep(gePollingDelay);
            } catch(InterruptedException e) { gePollingStatus=false; }
        }
    }
    
    /**
     * Terminate the polling loop
     */
    public void terminate() {
        /*
         Tells to the polling thread to exit from its loop
        */
        gePollingStatus = false;
    }             
    
}
    
