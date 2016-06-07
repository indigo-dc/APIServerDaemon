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
import org.apache.log4j.Logger;

/**
 * Runnable class responsible to execute APIServerDaemon commands
 * This class does not handle directly APIServer API calls but rather
 * uses <target>Interface class instances
 * The use of interface classes allow targeting other command 
 * executor services
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineInterface
 */
class APIServerDaemonProcessCommand implements Runnable {        
    
    APIServerDaemonCommand asdCommand;    
    String asdConnectionURL;        
    
    /*
      APIServerDaemon config
    */
    APIServerDaemonConfig asdConfig;        
    
    /**
     * Supported commands
     */
    private enum Commands {
         CLEAN
        ,SUBMIT     
        ,GETSTATUS // This command is directly handled by GE API Server
        ,GETOUTPUT // This command is directly handled by GE API Server
        ,JOBCANCEL
    }

    /*
      Logger
    */  
    private static final Logger _log = Logger.getLogger(APIServerDaemonProcessCommand.class.getName());
    
    public static final String LS = System.getProperty("line.separator");
    
    String threadName;
    
    /**
     * Constructor that retrieves the command to execute and the 
     * APIServerDaemon database connection URL, necessary to finalize 
     * executed commands
     * @param asdCommand
     * @param asdConnectionURL 
     */
    public APIServerDaemonProcessCommand( APIServerDaemonCommand asdCommand
                                         ,String gedConnectionURL) {
        this.asdCommand = asdCommand;
        this.asdConnectionURL = gedConnectionURL;
        this.threadName = Thread.currentThread().getName();
    }
    
    /**
     * Load APIServerDaemon configuration settings
     * @param asdConfig APIServerDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig asdConfig) {
        // Save all configs
        this.asdConfig=asdConfig;                        
    }

    /**
     * Execution of the APIServerCommand
     */
    @Override
    public void run() {
        _log.info("EXECUTING command: "+asdCommand);
        
        switch (Commands.valueOf(asdCommand.getAction())) {
            case CLEAN:     clean();
                break;
            case SUBMIT:    submit();
                break;
            case GETSTATUS: getStatus();
                break;
            case GETOUTPUT: getOutput();
                break;
            case JOBCANCEL: jobCancel();
                break;
            default:              
                _log.warn("Unsupported command: '"+asdCommand.getAction()+"'");
                // Set a final state for this command
                // todo ...
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
        _log.debug("Submitting command: "+asdCommand);
        
        switch (asdCommand.getTarget()) {
            case "GridEngine":
                GridEngineInterface geInterface = new GridEngineInterface(asdConfig,asdCommand);
                int AGIId = geInterface.jobSubmit(); // Currently this returns 0 
                                                     // AGIId is taken from checkCommand loop
                asdCommand.setStatus("PROCESSED");
                asdCommand.Update();
                _log.debug("Submitted command (GridEngine): "+asdCommand.toString());
                break;
            
            case "SimpleTosca":
                SimpleToscaInterface stInterface = new SimpleToscaInterface(asdConfig,asdCommand);
                int simple_tosca_id = stInterface.submitTosca();
                asdCommand.setTargetId(simple_tosca_id);
                asdCommand.setStatus("PROCESSED");
                asdCommand.Update();
                _log.debug("Submitted command (SimpleTosca): "+asdCommand.toString());
                break;
            
            //case "<other_target>"
            //    break;
                
            default:
                _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
                break;
        }
    }
    
    /**
     * Execute a GridEngineDaemon 'status' command
     * Asynchronous GETSTATUS commands should never come here
     */
    private void getStatus() {
        _log.debug("Get status command: "+asdCommand);
        if(asdCommand.getTarget().equals("GridEngine")) {
            GridEngineInterface geInterface = new GridEngineInterface(asdCommand);
            asdCommand.setTargetStatus(geInterface.jobStatus());
            asdCommand.setStatus("PROCESSED");
            asdCommand.Update();
        }/* else if(asdCommand.getTarget().equals(<other targets>)) {
        } */
        else {
            _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
        }
    }
    
    /**
     * Execute a GridEngineDaemon 'clean' command; just set PROCESSED
     * so that the Controller can process it
     */
    private void clean() {
        _log.debug("Clean command: "+asdCommand);
        if(asdCommand.getTarget().equals("GridEngine")) {
            GridEngineInterface geInterface = new GridEngineInterface(asdCommand);        
            asdCommand.setStatus("PROCESSED");
            asdCommand.Update();
        }/* else if(asdCommand.getTarget().equals(<other targets>)) {
        } */
        else {
            _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
        }
    }
    
    /**
     * Execute a GridEngineDaemon 'get output' command
     * Asynchronous GETOUTPUT commands should never come here
     */
    private void getOutput() {        
        _log.debug("Get output command: "+asdCommand);
        if(asdCommand.getTarget().equals("GridEngine")) {
            GridEngineInterface geInterface = new GridEngineInterface(asdCommand);
            asdCommand.setTargetStatus(geInterface.jobOutput());
            asdCommand.setStatus("PROCESSED");
            asdCommand.Update();
        }/* else if(asdCommand.getTarget().equals(<other targets>)) {
        } */
        else {
            _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
        }
    }
    
    /**
     * Execute a GridEngineDaemon 'job cancel' command
     */
    private void jobCancel() {
        _log.debug("Job cancel command: "+asdCommand);
        if(asdCommand.getTarget().equals("GridEngine")) {
            GridEngineInterface geInterface = new GridEngineInterface(asdCommand);
            geInterface.jobCancel();
            asdCommand.setStatus("PROCESSED");
            asdCommand.Update();
        }/* else if(asdCommand.getTarget().equals(<other targets>)) {
        } */
        else {
            _log.error("Unsupported target: '"+asdCommand.getTarget()+"'");
        }
    }   
}
