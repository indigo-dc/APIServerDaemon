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
import java.util.logging.Logger;

/**
 * Runnable class responsible to execute GridEngineDaemon commands
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
class GridEngineDaemonProcessCommand implements Runnable {        
    
    GridEngineDaemonCommand gedCommand;    
    String gedConnectionURL;
    
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
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    String threadName;
    
    /**
     * Constructor that retrieves the command to execute and the 
     * GridEngineDaemon database connection URL, necessary to finalize 
     * executed commands
     * @param gedCommand
     * @param gedConnectionURL 
     */
    public GridEngineDaemonProcessCommand(GridEngineDaemonCommand gedCommand
                                         ,String gedConnectionURL) {
        this.gedCommand = gedCommand;
        this.gedConnectionURL = gedConnectionURL;
        threadName = Thread.currentThread().getName();
    }

    /**
     * Execution of the GridEngineCommand
     */
    @Override
    public void run() {
        _log.info("EXECUTING command: "+gedCommand);
        
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
                _log.warning("Unsupported command: '"+gedCommand.getAction()+"'");
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
        _log.info("Submitting command: "+gedCommand);
        //GridEngineInterface geInterface = new GridEngineInterface(command);         
        //gedCommand.setAGIId(geInterface.jobSubmit());
        finalizeCommand("SUBMITTED");
    }
    
    /**
     * Execute a GridEngineDaemon 'status' command
     * Asynchronous GETSTATUS commands should never come here
     */
    private void getStatus() {
        _log.info("Get status command: "+gedCommand);
        GridEngineInterface geInterface = new GridEngineInterface(gedCommand);
        gedCommand.setStatus(geInterface.jobStatus());
        finalizeCommand(null);
    }
    
    /**
     * Execute a GridEngineDaemon 'get output' command
     * Asynchronous GETOUTPUT commands should never come here
     */
    private void getOutput() {
        _log.info("Get output command: "+gedCommand);
        GridEngineInterface geInterface = new GridEngineInterface(gedCommand);
        gedCommand.setStatus(geInterface.jobOutput());
        finalizeCommand(null);
    }
    
    /**
     * Execute a GridEngineDaemon 'job cancel' command
     */
    private void jobCancel() {
        _log.info("Job cancel command: "+gedCommand);
        GridEngineInterface geInterface = new GridEngineInterface(gedCommand);
        geInterface.jobCancel();
        finalizeCommand(null);
    }
    
    /**
     * Finalize the GridEngine command once completed
     */
    private void finalizeCommand(String status) {
        GridEngineDaemonDB gedDB = null;
        
        try {
                gedDB= new GridEngineDaemonDB(gedConnectionURL);
                gedDB.releaseCommand(gedCommand,status);                                
            } catch (Exception e) {
                /* Do something */
                _log.severe("Unable to get APIServer commands");
            }
            finally {
               if(gedDB!=null) gedDB.close(); 
            }
    }
}