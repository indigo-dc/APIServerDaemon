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

import java.lang.Runtime;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Runnable class responsible to check GridEngineDaemon commands
 * This class mainly checks for commands consistency and it does not 
 * handle directly GridEngine API calls but rather uses GridEngineInterface 
 * class instances
 * The use of an interface class may help targeting other command 
 * executor services if needed
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineInterface
 */
public class GridEngineDaemonCheckCommand implements Runnable  {        
    
    private GridEngineDaemonCommand gedCommand;    
    private String gedConnectionURL;      
    
    /*
      GridEngineDaemon config
    */
    GridEngineDaemonConfig gedConfig;        
    
    /**
     * Supported commands
     */
    private enum Commands {
         SUBMIT     
        ,GETSTATUS // This command is directly handled by GE API Server
        ,GETOUTPUT // This command is directly handled by GE API Server
        ,JOBCANCEL
    }

    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(GridEngineDaemonCheckCommand.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    String threadName;
    
    /**
     * Constructor that retrieves the command to execute and the 
     * GridEngineDaemon database connection URL, necessary to finalize 
     * executed commands
     * @param gedCommand
     * @param gedConnectionURL 
     */
    public GridEngineDaemonCheckCommand(GridEngineDaemonCommand gedCommand
                                         ,String gedConnectionURL) {
        this.gedCommand = gedCommand;
        this.gedConnectionURL = gedConnectionURL;
        threadName = Thread.currentThread().getName();
    }
    
    /**
     * Load GridEngineDaemon configuration settings
     * @param gedConfig GridEngineDaemon configuration object
     */
    public void setConfig(GridEngineDaemonConfig gedConfig) {
        // Save all configs
        this.gedConfig=gedConfig;                        
    }

    /**
     * Check of the GridEngineCommand
     * 
     * Checking depends by the couple (action,status)
     * Statuses taken by CheckCommand are: 
     *   PROCESSING: The command is being processed
     *    PROCESSED: The command has been processed
     * 
     * Action    | PROCESSING  | PROCESSED
     * ----------+-------------+-----------------
     * Submit    | Consistency | Check job status
     * ----------+-------------+-----------------
     * GetStatus |      -      |       -
     * ----------+-------------+-----------------
     * GetOutput |      -      |       -         
     * ----------+-------------+-----------------
     * JobCancel | Consistency | Check on GE
     * ----------+-------------+-----------------
     * 
     * GetStatus and GetOutput are synchronous operations
     * directly handled by the APIServer engine for this
     * reason these actions are not supported
     * Consistency check verifies how long the command waits
     * in order to be processed, if it takes too long
     * the command could be re-queued and/or tagged as FAILED
     * Check job status verifies the job status inside the
     * GridEngine' ActiveGridInteraction
     * Check on GE, verifies that job has been cancelled on
     * the GridEngine as well
     */
    @Override
    public void run() {
        _log.debug("Checking command: "+gedCommand);
        
        switch (Commands.valueOf(gedCommand.getAction())) {
            case SUBMIT:    submit();
                break;
            case GETSTATUS: getStatus();
                break;
            case GETOUTPUT: getOutput();
                break;
            case JOBCANCEL: jobCancel();
                break;
            default:              
                _log.warn("Unsupported command: '"+gedCommand.getAction()+"'");
                break;
        }                
    }
    
    /*
      Commands implementations
    */
    
    /**
     * Execute a GridEngineDaemon 'submit' command
     */
    private void submit() {
        _log.debug("Checking submitted command: "+gedCommand);        
        
        if(gedCommand.getStatus().equals("PROCESSING")) {
            // This check consistency of the command execution
            // if it takes too long the command should be 
            // resubmitted or flagged as FAILED reaching a threshold
            
        } else {
            // Status is PROCESSED; the job has been submited
            
            // First prepare the GridEngineInterface passing config
            GridEngineInterface geInterface = 
                    new GridEngineInterface(gedCommand);
            geInterface.setConfig(gedConfig);
            
            // Waiting for GridEngine update; the following code
            // retrieves the right agi_id field exploiting the
            // fixed jobDescription field inside the ActiveGridInteraction
            if(gedCommand.getAGIId() == 0) {
                gedCommand.setAGIId(geInterface.getAGIId());
                updateCommand();
            }
                        
            // Update ge_status taking its value from the GridEngine'
            // ActiveGridInteraction table, then if ge_status is DONE
            // flag also the command state to DONE allowing APIServer'
            // GetOutput call to work        
            if(gedCommand.getAGIId() != 0) {
                gedCommand.setGEStatus(geInterface.jobStatus());
                if(gedCommand.getGEStatus() != null
                && gedCommand.getGEStatus().equals("DONE")) {
                    gedCommand.setStatus("DONE");
                    // DONE command means that jobOutput is ready
                    String outputDir = geInterface.prepareJobOutput();
                    updateOutputPaths(outputDir);
                }
                updateCommand();
            }
        }        
    }
    
    /**
     * Execute a GridEngineDaemon 'status' command
     * Asynchronous GETSTATUS commands should never come here
     */
    private void getStatus() {
        _log.debug("Checkinig get status command: "+gedCommand);
    }
    
    /**
     * Execute a GridEngineDaemon 'get output' command
     * Asynchronous GETOUTPUT commands should never come here
     */
    private void getOutput() {
        _log.debug("Check get output command: "+gedCommand);
    }
    
    /**
     * Execute a GridEngineDaemon 'job cancel' command
     */
    private void jobCancel() {
        _log.debug("Check job cancel command: "+gedCommand);
    }
        
    /**
     * Finalize the GridEngine command once processed
     */
    private void updateCommand() {
        GridEngineDaemonDB gedDB = null;
        
        if(gedCommand.isModified())
            try {
                    gedDB= new GridEngineDaemonDB(gedConnectionURL);
                    gedDB.updateCommand(gedCommand);
                    gedCommand.validate();
            } catch (Exception e) {              
                _log.fatal("Unable update command:"+LS+gedCommand
                                                   +LS+e.toString());
            }
            finally {
               if(gedDB!=null) gedDB.close(); 
            }
    }
    
    /**
     * Update task' output file paths
     */
    void updateOutputPaths(String outputDir) {
        GridEngineDaemonDB gedDB = null;
                
        try {
                gedDB= new GridEngineDaemonDB(gedConnectionURL);
                gedDB.updateOutputPaths(gedCommand,outputDir);                
        } catch (Exception e) {
          //_log.severe("Unable release command:"+LS+gedCommand
            _log.fatal("Unable release command:"+LS+gedCommand
                                                 +LS+e.toString());
        }
        finally {
           if(gedDB!=null) gedDB.close(); 
        }
    }
}