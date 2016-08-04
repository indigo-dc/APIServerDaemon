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

//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * This is the Runnable class that implements the polling thread This class
 * implements one of the two principal GridEngineDaemon threads together with
 * APIServerDaemonPolling class
 * 
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemonController
 */
class APIServerDaemonPolling implements Runnable {

    /*
     * Logger
     */
    private static final Logger _log = Logger.getLogger(APIServerDaemonPolling.class.getName());
    public static final String LS = System.getProperty("line.separator");

    /*
     * GridEngineDaemon Polling settings
     */
    private boolean asPollingStatus = true;

    /*
     * Thread pool executor
     */
    private ExecutorService asdExecutor = null;

    /*
     * GridEngine API Server Database settings
     */
    private String apisrv_dbname;
    private String apisrv_dbhost;
    private String apisrv_dbport;
    private String apisrv_dbuser;
    private String apisrv_dbpass;
    private int asPollingDelay;
    private int asPollingMaxCommands;

    /*
     * GridEngineDaemon config
     */
    APIServerDaemonConfig asdConfig;
    String threadName;

    /**
     * Constructor receiving the threadpool executor object allowing this class
     * to submit further threads
     * 
     * @param asdExecutor
     *            The executor object instantiated from GridEngineDaemon class
     * @see GridEngineDaemon
     */
    public APIServerDaemonPolling(ExecutorService asdExecutor) {
	this.asdExecutor = asdExecutor;
	threadName = Thread.currentThread().getName();
	_log.info("Initializing APIServer PollingThread");
    }

    /**
     * APIServerDaemonPolling 'run' method loops until asPollingStatus is true
     * Polling loops takes only WAITING status records from the as_queue table
     * and then process them with the APIServerDaemonProcessCommand The same
     * kind of loop exists in the APIServerDaemonController
     * 
     * @see APIServerDaemonProcessCommand
     * @see APIServerDaemonController
     */
    @Override
    public void run() {
	APIServerDaemonDB asdDB = null;

	_log.info("Starting APIServer PollingThread");

	/*
	 * PollingThread main loop; it gets available commands from queue
	 */
	while (asPollingStatus) {
	    try {

		/*
		 * Retrieves commands from DB
		 */
		asdDB = new APIServerDaemonDB(apisrv_dbhost, apisrv_dbport, apisrv_dbuser, apisrv_dbpass,
			apisrv_dbname);

		List<APIServerDaemonCommand> commands = asdDB.getQueuedCommands(asPollingMaxCommands);

		_log.debug("Received " + commands.size() + "/" + asPollingMaxCommands + " waiting commands");

		/*
		 * Process retrieved commands
		 */
		Iterator<APIServerDaemonCommand> iterCmds = commands.iterator();

		while (iterCmds.hasNext()) {
		    APIServerDaemonCommand asdCommand = iterCmds.next();
		    APIServerDaemonProcessCommand asdProcCmd = new APIServerDaemonProcessCommand(asdCommand,
			    asdDB.getConnectionURL());

		    if (asdProcCmd != null) {
			asdProcCmd.setConfig(asdConfig);
			asdExecutor.execute(asdProcCmd);
		    }
		}
	    } catch (Exception e) {
		_log.fatal("Unable to get APIServer commands");
	    } finally {
		// if (asdDB != null) {
		// asdDB.close();
		// }
	    }

	    /*
	     * Wait for next loop
	     */
	    try {
		Thread.sleep(asPollingDelay);
	    } catch (InterruptedException e) {
		asPollingStatus = false;
	    }
	}
    }

    /**
     * Terminate the polling loop
     */
    public void terminate() {

	/*
	 * Tells to the polling thread to exit from its loop
	 */
	asPollingStatus = false;
    }

    /**
     * Load APIServerDaemon configuration settings
     * 
     * @param asdConfig
     *            APIServerDaemon configuration object
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

	// Load GridEngineDaemon settings
	this.asPollingDelay = asdConfig.getPollingDelay();
	this.asPollingMaxCommands = asdConfig.getPollingMaxCommands();
	_log.info("APIServerDaemon config:" + LS + "  [Database]" + LS + "    db_host: '" + this.apisrv_dbhost + "'"
		+ LS + "    db_port: '" + this.apisrv_dbport + "'" + LS + "    db_user: '" + this.apisrv_dbuser + "'"
		+ LS + "    db_pass: '" + this.apisrv_dbpass + "'" + LS + "    db_name: '" + this.apisrv_dbname + "'"
		+ LS + "  [Polling config]" + LS + "    asPollingDelay  : '" + this.asPollingDelay + "'" + LS
		+ "    asPollingMaxCommands: '" + this.asPollingMaxCommands + "'" + LS);
    }
}
