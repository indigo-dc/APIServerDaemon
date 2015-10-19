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

// Importing GridEngine Job libraries
import it.infn.ct.GridEngine.Job.*;
import it.infn.ct.GridEngine.JobResubmission.GEJobDescription;
import it.infn.ct.GridEngine.Job.MultiInfrastructureJobSubmission;
import it.infn.ct.GridEngine.UsersTracking.UsersTrackingDBInterface;
import java.net.InetAddress;

import java.util.logging.Logger;

/**
 * This class interfaces any call to the GridEngine library
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineInterface {
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    GridEngineDaemonCommand gedCommand;
    
    /**
     * Empty constructor for GridEngineInterface
     */
    public GridEngineInterface() {
        _log.info("Initializing empty GridEngineInterface");
    }
    /**
     * Constructor for GridEngineInterface taking as input a given command
     */
    public GridEngineInterface(GridEngineDaemonCommand gedCommand) {
        this.gedCommand=gedCommand;
        _log.info("Initialized GridEngineInterface with command: "+LS
                  +this.gedCommand);
    }
    
    /*
      GridEngine interfacing methods
    */
    
    /**
     * submit the job identified by the gedCommand values
     */
    public int jobSubmit() {
        int agi_id=0;        
        _log.info("Submitting job");
/*
        // Retrieve the full  path to the job directory
        String jobPath = gedCommand.getActionInfo();
        InfrastructureInfo infrastructure;
        MultiInfrastructureJobSubmission mijs = new MultiInfrastructureJobSubmission();
        
        infrastructure = new InfrastructureInfo(cpinfra.name
                                               ,cpinfra.adaptor
                                               ,""
                                               ,cpinfra.resourceList()
                                               ,cpinfra.getParam("etoken_host")
                                               ,cpinfra.getParam("etoken_port")
                                               ,cpinfra.getParam("etoken_id")
                                               ,cpinfra.getParam("VO")
                                               ,cpinfra.getParam("VO_GroupRole")
                                               ,cpinfra.getParam("ProxyRFC").equalsIgnoreCase("true")
                                               );
        
        mijs.addInfrastructure(infrastructure);
         GE_JobId = "'" + alephfileName + "'";
        // Set job properties
        mijs.setExecutable("aleph.sh");                              // Executable
        mijs.setArguments("");
        mijs.setJobOutput("stdout.txt");                             // std-output
        mijs.setJobError("stderr.txt");                              // std-error
        mijs.setOutputPath("/tmp/");                                 // Output path
        mijs.setInputFiles("");                                         // InputSandbox
        mijs.setOutputFiles("aleph_output.tar");                     // OutputSandbox

        // Determine the host IP address
        String   portalIPAddress="";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            byte[] ipAddr=addr.getAddress();
            portalIPAddress= ""+(short)(ipAddr[0]&0xff)
                           +":"+(short)(ipAddr[1]&0xff)
                           +":"+(short)(ipAddr[2]&0xff)
                           +":"+(short)(ipAddr[3]&0xff);
        }
        catch(Exception e) {
            _log.severe("Unable to get the portal IP address");
        }

        // Submit the job
        // Submission uses addInfrastructure method; this call is no longer necessary
        // mijs.submitJobAsync(infrastructure, username, portalIPAddress, alephGridOperation, GE_JobId);
        agi_id = mijs.submitJobAsync(username, portalIPAddress, alephGridOperation, GE_JobId,true);

        // Remove proxy temporary file
        // temp.delete(); Cannot remove here the file, job submission fails

        // Interactive job execution (iservices)
        if(isAlephVMEnabled && alephAlg == null) {
            iSrv.allocService(username,vmuuid);
            iSrv.dumpAllocations();
        }
*/
        return agi_id;
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public String jobStatus() {
        _log.info("Getting job status");
        return "NOTIMPLEMENTED";
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public String jobOutput() {
        _log.info("Getting job output");
        return "NOTIMPLEMENTED";
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public void jobCancel() {
        _log.info("Cancelling job");
        return;
    }
}
