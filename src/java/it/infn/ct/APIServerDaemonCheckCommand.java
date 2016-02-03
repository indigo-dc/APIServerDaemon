/**
 * ************************************************************************
 * Copyright (c) 2011: Istituto Nazionale di Fisica Nucleare (INFN), Italy
 * Consorzio COMETA (COMETA), Italy
 *
 * See http://www.infn.it and and http://www.consorzio-cometa.it for details on
 * the copyright holders.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
***************************************************************************
 */
package it.infn.ct;

import java.lang.Runtime;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Runnable class responsible to check APIServerDaemon commands This class
 * mainly checks for commands consistency and it does not handle directly
 * APIServer API calls but rather uses APIServerInterface class instances The
 * use of an interface class may help targeting other command executor services
 * if needed
 *
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineInterface
 */
public class APIServerDaemonCheckCommand implements Runnable {

    private APIServerDaemonCommand asdCommand;
    private String asdConnectionURL;

    /*
     APIServerDaemon config
     */
    APIServerDaemonConfig asdConfig;

    /**
     * Supported commands
     */
    private enum Commands {
          CLEAN     // This command cleans any task entry
        , SUBMIT
        , GETSTATUS // This command is directly handled by GE API Server
        , GETOUTPUT // This command is directly handled by GE API Server
        , JOBCANCEL
    }

    /*
     Logger
     */
    private static final Logger _log = Logger.getLogger(APIServerDaemonCheckCommand.class.getName());

    public static final String LS = System.getProperty("line.separator");

    String threadName;

    /**
     * Constructor that retrieves the command to execute and the
     * APIServerDaemon database connection URL, necessary to finalize executed
     * commands
     *
     * @param asdCommand
     * @param asdConnectionURL
     */
    public APIServerDaemonCheckCommand(APIServerDaemonCommand asdCommand, String asdConnectionURL) {
        this.asdCommand = asdCommand;
        this.asdConnectionURL = asdConnectionURL;
        this.threadName = Thread.currentThread().getName();
    }

    /**
     * Load APIServerDaemon configuration settings
     *
     * @param asdConfig APIServerDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig asdConfig) {
        // Save all configs
        this.asdConfig = asdConfig;
    }

    /**
     * Check of the APIServerCommand
     *
     * Checking depends by the couple (action,status) Statuses taken by
     * CheckCommand are: PROCESSING: The command is being processed PROCESSED:
     * The command has been processed
     *
     * Action    | PROCESSING | PROCESSED        | Target
     * ----------+-------------+-----------------+-----------
     * Submit    | Consistency | Check job status|    (*)
     * ----------+-------------+-----------------+-----------
     * GetStatus |      -      |       -         |
     * ----------+-------------+-----------------+-----------
     * GetOutput |      -      |       -         |
     * ----------+-------------+-----------------+-----------
     * JobCancel | Consistency | Check on GE     |
     * ----------+-------------+-----------------+-----------
     *
     * (*) GridEngine,JSAGA,pySAGA,EUDAT, ...
     *     Any target may have different Action/Status values
     * 
     * GetStatus and GetOutput are synchronous operations directly handled by
     * the APIServer engine for this reason these actions are not supported
     * directly
     * Consistency check verifies how long the command waits in order to be
     * processed, if it takes too long the command could be re-queued and/or
     * tagged as FAILED. 
     * 
     * Check job status verifies the job status inside the specific target
     * For isntance in the GridEngine' case it will be checked the 
     * ActiveGridInteraction table. This will verify that job has been
     * cancelled on the GridEngine as well
     * Same mechanisms can be applied to other interfaces
     * 
     */
    @Override
    public void run() {
        _log.debug("Checking command: " + asdCommand);

        switch (Commands.valueOf(asdCommand.getAction())) {
            case CLEAN:
                clean();
                break;
            case SUBMIT:
                submit();
                break;
            case GETSTATUS:
                getStatus();
                break;
            case GETOUTPUT:
                getOutput();
                break;
            case JOBCANCEL:
                jobCancel();
                break;
            default:
                _log.warn("Unsupported command: '" + asdCommand.getAction() + "'");
                break;
        }
    }
    
    /**
     * Clean everything associated to the task
     * I/O Sandbox and any DB allocation
     */
    private void clean() { 
        // Remove any API Server task entries including the queue
        removeTaksEntries();        
        // Remove info directory
        removeInfoDir();        
    }

    /*
     Commands implementations
     */
    /**
     * Check a APIServerDaemon 'submit' command
     */
    private void submit() {
        _log.debug("Checking submitted command: " + asdCommand);
        // Add a check for long lasting PROCESSING commands
        
        // Check PROCESSED commands
        if (asdCommand.getStatus().equals("PROCESSED")) {
            // Verify the TargetId exists then check the status
            // If the TargetId does not exists then check consistency
            if(asdCommand.getTarget().equals("GridEngine")) {
                // Status is PROCESSED; the job has been submited
                // First prepare the GridEngineInterface passing config
                GridEngineInterface geInterface
                        = new GridEngineInterface(asdConfig,asdCommand);                

                // Retrieve the right agi_id field exploiting the
                // fixed jobDescription field inside the ActiveGridInteraction
                // AGIId may change during submission in casethe job is
                // resubmitted by the GridEngine
                int AGIId = geInterface.getAGIId();
                _log.debug("AGIId for command having id:"+asdCommand.getTaskId()+" is: "+AGIId);
                asdCommand.setTargetId(geInterface.getAGIId());
                asdCommand.Update(asdConnectionURL);

                // Update target_status taking its value from the GridEngine'
                // ActiveGridInteraction table, then if target_status is DONE
                // flag also the command state to DONE allowing APIServer'
                // GetOutput call to work        
                if (asdCommand.getTargetId() != 0) {
                    String geJobStatus = geInterface.jobStatus();
                    _log.debug("Status of job "
                            + asdCommand.getTaskId() + " is '"
                            + geJobStatus + "'");
                    asdCommand.setTargetStatus(geJobStatus);
                    if (   asdCommand.getTargetStatus() != null
                        && asdCommand.getTargetStatus().equals("DONE")) {
                        asdCommand.setStatus("DONE");                        
                        // DONE command means that jobOutput is ready
                        String outputDir = geInterface.prepareJobOutput();
                        updateOutputPaths(outputDir);
                    }
                    asdCommand.Update(asdConnectionURL);
                } else {
                    // TargetId is 0 - check consistency ...
                    // This check consistency of the command execution
                    // if it takes too long the command should be 
                    // resubmitted or flagged as FAILED reaching a given
                    // threshold
                    // Tasks will be retryed if creation and last change is
                    // greater than max_wait and retries have not reached yet
                    // the max_retry count
                    // Trashed requests will be flagged as FAILED
                    _log.debug("Consistency of task - id: "+asdCommand.getTaskId()
                              +      " lifetime: "+asdCommand.getLifetime()
                              +                "/"+asdConfig.getTaskMaxWait()
                              +       " - retry: "+asdCommand.getRetry()
                              +                "/"+asdConfig.getTaskMaxRetries());
                    if(   asdCommand.getRetry()    < asdConfig.getTaskMaxRetries()
                       && asdCommand.getLifetime() > asdConfig.getTaskMaxWait()) {
                        _log.debug("Retrying task having id: "+asdCommand.getTaskId());
                        asdCommand.retry(asdConnectionURL);
                    } else if (asdCommand.getRetry() >= asdConfig.getTaskMaxRetries()) {
                            _log.debug("Trashing task having id: "+asdCommand.getTaskId());
                            asdCommand.trash(asdConnectionURL);
                    } else _log.debug("Ignoring at the moment task having id: "+asdCommand.getTaskId());                    
                }
            }/* else if(asdCommand.getTarget().equals(<other targets>)) {
                // Get/Use targetId to check task submission
                // If targetId does not appear after a long while check consistency
                // and eventually retry task submission.
            }*/ else {
                _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
            }         
        }
    }

    /**
     * Execute a APIServerDaemon 'status' command Asynchronous GETSTATUS
     * commands should never come here
     */
    private void getStatus() {
        _log.debug("Checkinig get status command: " + asdCommand);
    }

    /**
     * Execute a APIServerDaemon 'get output' command Asynchronous GETOUTPUT
     * commands should never come here
     */
    private void getOutput() {
        _log.debug("Check get output command: " + asdCommand);
    }

    /**
     * Execute a APIServerDaemon 'job cancel' command
     */
    private void jobCancel() {
        _log.debug("Check job cancel command: " + asdCommand);
    }

    /**
     * Update task' output file paths
     */
    void updateOutputPaths(String outputDir) {
        APIServerDaemonDB asdDB = null;

        try {
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.updateOutputPaths(asdCommand, outputDir);
        } catch (Exception e) {
            //_log.severe("Unable release command:"+LS+asdCommand
            _log.fatal("Unable release command:" + LS + asdCommand
                    + LS + e.toString());
        } finally {
            if (asdDB != null) {
                asdDB.close();
            }
        }
    }
    
    /**
     * Remove any task entry from the DB including the queue record
     */
    private void removeTaksEntries() {
        APIServerDaemonDB asdDB = null;

        try {
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.removeTaksEntries(asdCommand.getTaskId());
        } catch (Exception e) {            
            _log.fatal("Unable to remove task entries for command:" + LS + asdCommand
                    + LS + e.toString());
        } finally {
            if (asdDB != null) {
                asdDB.close();
            }
        }
    }
    
    /**
     * Remove actionInfo directory from the file system
     */
    private void removeInfoDir() {
        String infoDir = asdCommand.getActionInfo();
        try {
            Process delInfoDir = 
                    Runtime.getRuntime().exec("rm -rf "+infoDir);                                         
            delInfoDir.waitFor();
        } catch (Exception e) {          
            _log.fatal("Error removing infoDIR: '"+infoDir+"'");
        }
    }
}
