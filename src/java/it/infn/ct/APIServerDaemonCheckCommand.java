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
    public static final String FS = System.getProperty("file.separator");

    String threadName;

    /**
     * Constructor that retrieves the command to execute and the
     * APIServerDaemon database connection URL, necessary to finalize executed
     * commands
     *
     * @param asdCommand
     * @param asdConnectionURL
     */
    public APIServerDaemonCheckCommand(APIServerDaemonCommand asdCommand) {
        this.asdCommand = asdCommand;      
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
        switch (asdCommand.getStatus()) {
            // PROCESSING - The command have been taken from the task
            // queue and provided to its target executor
            case "PROCESSING":
                // Verify how long the task remains in PROCESSING state
                // if longer than MaxWait, retry the command if the number
                // of retries did not reached yet the MaxRetries value
                // otherwise trash the task request (marked as FAILED)
                
                // Provide a different behavior depending on the Target
                if(asdCommand.getTarget().equals("GridEngine")) {  
                    taskConsistencyCheck();
                } else if(asdCommand.getTarget().equals("SimpleTosca")) {
                    // Disable at the moment consistency check
                    //taskConsistencyCheck();
                } /* else if(asdCommand.getTarget().equals(<other targets>)) {
                // Get/Use targetId to check task submission
                // If targetId does not appear after a long while check consistency
                // and eventually retry task submission.
                }*/
                else {
                    _log.warn("Unsupported target: '"+asdCommand.getTarget()+"'");
                }   
            break;
                
            // The task target executor processed requested task
            case "PROCESSED":
                // Verify that TargetId exists, if yes check the status
                // otherwise check task consistency
                
                // Provide a different behavior depending on the Target
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
                    asdCommand.setTargetId(AGIId);
                    asdCommand.Update();
                    
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
                        asdCommand.Update();
                    } else {
                        // TargetId is 0 - check consistency ...
                        taskConsistencyCheck();
                    }
                } else if(asdCommand.getTarget().equals("SimpleTosca")) {
                    // Determine the status and take care of the output files
                    SimpleToscaInterface stInterface
                            = new SimpleToscaInterface(asdConfig,asdCommand);
                    asdCommand.setStatus("HOLD"); // Avoid during check that futher checks occur
                    asdCommand.Update();
                    String currState="PROCESSED"; // Status after HOLD
                    String status = stInterface.getStatus();
                    if(status != null && status.length() > 0) {
                        asdCommand.setTargetStatus(status);                      
                        if(status=="DONE") {
                            currState = status;
                            updateOutputPaths(SimpleToscaInterface.getOutputDir());                            
                        } 
                    } else {
                        _log.warn("No status available yet");                        
                        // No status is available - check consistency ...
                        //!skip consistency at the moment taskConsistencyCheck();
                    }
                    asdCommand.setStatus(currState); // Setup the current state
                    asdCommand.Update();
                } /* else if(asdCommand.getTarget().equals(<other targets>)) {
                // Get/Use targetId to check task submission
                // If targetId does not appear after a long while check consistency
                // and eventually retry task submission.
                }*/ else {
                    _log.warn("Unsupported target: '"+asdCommand.getTarget()+"'");
                }   
            break;
            default:
                _log.error("Ignoring unsupported status: '"+asdCommand.getStatus()+"' for task: "+asdCommand.getTaskId());
        } // switch on STATUS
           
        // Updating check_ts field a round-robing strategy will be
        // applied while extracting command from the queue by controller
        asdCommand.checkUpdate();
    }

    /**
     * Check the consistency of the given command
     */
    private void taskConsistencyCheck() {
        // This check consistency of the command execution
        // if it takes too long the command should be
        // resubmitted or flagged as FAILED reaching a given
        // threshold
        // Tasks will be retryed if creation and last change is
        // greater than max_wait and retries have not reached yet
        // the max_retry count
        // Trashed requests will be flagged as FAILED
        _log.debug("Consistency of PROCESSED task - id: "+asdCommand.getTaskId()
                                     +      " lifetime: "+asdCommand.getLifetime()
                                     +                "/"+asdConfig.getTaskMaxWait()
                                     +       " - retry: "+asdCommand.getRetry()
                                     +                "/"+asdConfig.getTaskMaxRetries());
        if(   asdCommand.getRetry()    < asdConfig.getTaskMaxRetries()
           && asdCommand.getLifetime() > asdConfig.getTaskMaxWait()) {
            _log.debug("Retrying PROCESSED task having id: "+asdCommand.getTaskId());
            asdCommand.retry();
        } else if (asdCommand.getRetry() >= asdConfig.getTaskMaxRetries()) {
            _log.debug("Trashing PROCESSED task having id: "+asdCommand.getTaskId());
            asdCommand.trash();
        } else _log.debug("Ignoring at the moment PROCESSED task having id: "+asdCommand.getTaskId());
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
            asdDB = new APIServerDaemonDB(asdCommand.getASDConnectionURL());
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
                
        _log.debug("Removing task: '"+asdCommand.getTaskId()+"'");
                
        // Now remove task entries in APIServer DB
        try {
            // First take care to remove specific target entries
            switch(asdCommand.getTarget()) {
                case "GridEngine":                
                    // First prepare the GridEngineInterface passing config
                    GridEngineInterface geInterface
                            = new GridEngineInterface(asdConfig,asdCommand);                    
                    // Retrieve the right agi_id field exploiting the
                    // fixed jobDescription field inside the ActiveGridInteraction
                    // AGIId may change during submission in casethe job is
                    // resubmitted by the GridEngine
                    int AGIId = geInterface.getAGIId();
                    _log.debug("AGIId for command having id:"+asdCommand.getTaskId()+" is: "+AGIId);
                    asdCommand.setTargetId(AGIId);
                    asdCommand.Update();
                    // Now verify if target exists
                    if(asdCommand.getTargetId() > 0) {
                        _log.debug("Removing record for GridEngine: '"+asdCommand.getTaskId()+"' -> AGI: '"+asdCommand.getTargetId()+"'");
                        // Task should be in RUNNING state
                        if(asdCommand.getStatus().equals("RUNNING"))
                                _log.warn("Removing a GridEngine' RUNNING task, its job execution will be lost");                        
                        geInterface.removeAGIRecord(geInterface.getAGIId());
                    } else {
                        _log.debug("No GridEngine ActiveGridInteraction record is asssociated to the task: '"+asdCommand.getTaskId()+"'");
                    }
                    break;
                // Place other targets below ...
                // case "mytarget":
                // break;
                default:
                    _log.warn("Unrecognized target '"+asdCommand.getTarget()+"' while deleting target specific task entries");
            }
            
            // Now remove APIServer DB task entries
            asdDB = new APIServerDaemonDB(asdCommand.getASDConnectionURL());
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
