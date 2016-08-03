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
import java.util.Observable;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

/**
 * Runnable class that controls APIServerDaemon activities such as:
 * - Update job status values of any submitted task
 * - Manage job output request
 * - Preserve consistency status of any broken activity
 * - Cleanup done operations
 * This class implements one of the two principal APIServerDaemon threads
 * together with APIServerDaemonPolling class
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see APIServerDaemonPolling
 */
public class APIServerDaemonController extends Observable implements Runnable {

    /*
     * Logger
     */
    private static final Logger _log = Logger.getLogger(APIServerDaemonController.class.getName());
    public static final String  LS   = System.getProperty("line.separator");

    /*
     * APIServerDaemon Controller settings
     */
    private boolean asControllerStatus = true;

    /*
     * Thread pool executor
     */
    private ExecutorService asdExecutor = null;

    /*
     * APIServer API Server Database settings
     */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    private int    asControllerDelay;
    private int    asControllerMaxCommands;

    /*
     * APIServerDaemon config
     */
    APIServerDaemonConfig asdConfig;
    String                threadName;

    /**
     * Instantiate a APIServerDaemonController allowing to execute further
     * threads using the given Executor object
     * @param asdExecutor Executor object created by the APIServerDaemon
     */
    public APIServerDaemonController(ExecutorService asdExecutor) {
        this.asdExecutor = asdExecutor;
        threadName       = Thread.currentThread().getName();
        _log.info("Initializing APIServer PollingThread");
    }

    @Override
    public void run() {
        APIServerDaemonDB asdDB = null;

        _log.info("Starting APIServer ControllerThread");

        /**
         * APIServerDaemonController 'run' method loops until geControllerStatus
         * is true
         * Polling loops takes only the following kind of command statuses
         * from the as_queue:
         *  - PROCESSING: Verify time consistency retrying command if necessary
         *  - SUBMITTED : as above
         *  - RUNNING   : as above
         *  - DONE      : Cleanup allocated space for expired tasks
         * table and then process them with the GrirEngineDaemonProcessCommand
         * The same kind of loop exists in the GridEngineDaemonPolling
         * @see APIServerDaemonProcessCommand
         * @see APIServerDaemonPolling
         */
        while (asControllerStatus) {
            try {

                /*
                 * Retrieves commands from DB
                 */
                asdDB = new APIServerDaemonDB(apisrv_dbhost,
                                              apisrv_dbport,
                                              apisrv_dbuser,
                                              apisrv_dbpass,
                                              apisrv_dbname);

                List<APIServerDaemonCommand> commands = asdDB.getControllerCommands(asControllerMaxCommands);

                _log.debug("Received " + commands.size() + "/" + asControllerMaxCommands + " controller commands");

                /*
                 * Process retrieved commands
                 */
                Iterator<APIServerDaemonCommand> iterCmds = commands.iterator();

                while (iterCmds.hasNext()) {
                    APIServerDaemonCommand      asdCommand  = iterCmds.next();
                    APIServerDaemonCheckCommand asdCheckCmd = new APIServerDaemonCheckCommand(asdCommand);

                    if (asdCheckCmd != null) {
                        asdCheckCmd.setConfig(asdConfig);
                        asdExecutor.execute(asdCheckCmd);
                    }
                }
            } catch (Exception e) {

                /* Do something */
                _log.fatal("Unable to get APIServer commands");
            } finally {
                //if (asdDB != null) {
                //    asdDB.close();
                //}
            }

            /*
             * Wait for next loop
             */
            try {
                Thread.sleep(asControllerDelay);
            } catch (InterruptedException e) {
                asControllerStatus = false;
            }
        }
    }

    /**
     * Terminate the controller loop
     */
    public void terminate() {

        /*
         * Tells to the controller thread to exit from its loop
         */
        asControllerStatus = false;
        notifyObservers();
    }

    /**
     * Load APIServerDaemon configuration settings
     * @param asdConfig APIServerDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig asdConfig) {

        // Save configs
        this.asdConfig = asdConfig;

        // Set configuration values for this class
        this.apisrv_dbhost = asdConfig.getApisrv_dbhost();
        this.apisrv_dbport = asdConfig.getApisrv_dbport();
        this.apisrv_dbuser = asdConfig.getApisrv_dbuser();
        this.apisrv_dbpass = asdConfig.getApisrv_dbpass();
        this.apisrv_dbname = asdConfig.getApisrv_dbname();

        // Load APIServerDaemon settings
        this.asControllerDelay       = asdConfig.getControllerDelay();
        this.asControllerMaxCommands = asdConfig.getControllerMaxCommands();
        _log.info("APIServerDaemon config:" 
                  + LS + "  [Database]" 
                  + LS + "    db_host: '" + this.apisrv_dbhost + "'"
                  + LS + "    db_port: '" + this.apisrv_dbport + "'" 
                  + LS + "    db_user: '" + this.apisrv_dbuser + "'"
                  + LS + "    db_pass: '" + this.apisrv_dbpass + "'" 
                  + LS + "    db_name: '" + this.apisrv_dbname + "'"
                  + LS + "  [Controller config]" 
                  + LS + "    ControllerDelay      : '" + this.asControllerDelay + "'"
                  + LS + "    ControllerMaxCommands: '" + this.asControllerMaxCommands + "'" + LS);
    }
}
