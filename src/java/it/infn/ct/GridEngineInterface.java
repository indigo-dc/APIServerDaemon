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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.logging.Logger;
import org.json.*;
import org.apache.commons.io.IOUtils;

/**
 * This class interfaces any call to the GridEngine library
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineInterface {
    /*
     GridEngine UsersTracking DB
    */    
    private String utdb_host;
    private String utdb_port;
    private String utdb_user;
    private String utdb_pass;
    private String utdb_name;
    /*
      GridEngineDaemon config
    */
    GridEngineDaemonConfig gedConfig;
    /*
      GridEngineDaemon IP address
    */
    String gedIPAddress;
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
        getIP();
    }
    /**
     * Constructor for GridEngineInterface taking as input a given command
     */
    public GridEngineInterface(GridEngineDaemonCommand gedCommand) {
        this();
        this.gedCommand=gedCommand;
        _log.info("Initialized GridEngineInterface with command: "+LS
                  +this.gedCommand);        
    }
    
    /*
      GridEngine interfacing methods
    */
    
    /**
     * Load GridEngineDaemon configuration settings
     * @param gedConfig GridEngineDaemon configuration object
     */
    public void setConfig(GridEngineDaemonConfig gedConfig) {
        this.gedConfig=gedConfig;                
        // Extract class specific configutation            
        this.utdb_host = gedConfig.getGridEngine_db_host();
        this.utdb_port = gedConfig.getGridEngine_db_port();
        this.utdb_user = gedConfig.getGridEngine_db_user();
        this.utdb_pass = gedConfig.getGridEngine_db_pass();
        this.utdb_name = gedConfig.getGridEngine_db_name();
        _log.info("GridEngineInterface config:"            +LS
                 +"  [UsersTrackingDB]"                    +LS
                 +"    db_host: '"      +this.utdb_host+"'"+LS
                 +"    db_port: '"      +this.utdb_port+"'"+LS
                 +"    db_user: '"      +this.utdb_user+"'"+LS
                 +"    db_pass: '"      +this.utdb_pass+"'"+LS
                 +"    db_name: '"      +this.utdb_name+"'"+LS);
    }
    
    /**
     * Setup machine IP address, needed by job submission
     */
    private void getIP() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            byte[] ipAddr=addr.getAddress();
            gedIPAddress= ""+(short)(ipAddr[0]&0xff)
                         +":"+(short)(ipAddr[1]&0xff)
                         +":"+(short)(ipAddr[2]&0xff)
                         +":"+(short)(ipAddr[3]&0xff);
        }
        catch(Exception e) {
            _log.severe("Unable to get the portal IP address");
        }
    }
    
    /**
     * submit the job identified by the gedCommand values
     * @return 
     */
    public int jobSubmit() {
        int agi_id=0;        
        _log.info("Submitting job");
        MultiInfrastructureJobSubmission mijs =
            new MultiInfrastructureJobSubmission(
                    "jdbc:mysql://"+utdb_host+":"
                                   +utdb_port+"/"
                                   +utdb_name
                    ,utdb_user
                    ,utdb_pass
            );
        if(mijs==null)
            _log.info("mijs is NULL, sorry!");       
        else try {            
            _log.info("Loading GridEngine job JSON desc");
            JSONObject jsonJobDesc = loadJSONJobDesc();
            // application
            int geAppId = jsonJobDesc.getInt("application");
            // commonName
            String geCommonName = jsonJobDesc.getString("commonName");
            // infrastructure
            JSONObject geInfrastructure = 
                    jsonJobDesc.getJSONObject("infrastructure");
            // jobDescription
            JSONObject geJobDescription = 
                    jsonJobDesc.getJSONObject("jobDescription");
            // credentials
            JSONObject geCredentials = 
                    jsonJobDesc.getJSONObject("credentials");
            // identifier
            String jobIdentifier =
                    jsonJobDesc.getString("identifier");
            // Loaded essential JSON components; now go through
            // each adaptor specific setting:
            // resourceManagers
            String  resourceManagers = 
                    geInfrastructure.getString("resourceManagers");
            String adaptor = resourceManagers.split(":")[0];
            _log.info("Adaptor is '"+adaptor+"'");
            InfrastructureInfo infrastructures[] = new InfrastructureInfo[1];
            switch(adaptor) {
                // SSH Adaptor
                case "ssh": 
                    try {
                        _log.info("Entering SSH adaptor ...");
                        String  username = 
                            geCredentials.getString("username");
                        String  password = 
                            geCredentials.getString("password");
                        String sshEndPoing[] = { resourceManagers };
                        infrastructures[0] = new InfrastructureInfo(
                                 resourceManagers
                               , "ssh"
                               , username
                               , password
                               , sshEndPoing);
                        mijs.addInfrastructure(infrastructures[0]);                    
                        // Job description
                        mijs.setExecutable(
                            geJobDescription.getString("executable"));
                        mijs.setJobOutput(
                            geJobDescription.getString("output"));
                        mijs.setArguments(
                            geJobDescription.getString("arguments"));
                        mijs.setJobError(
                            geJobDescription.getString("error"));
                        mijs.setOutputPath(gedCommand.getActionInfo());                        
                        // IO Files
                        //description.setInputFiles("/home/mario/Documenti/hostname.sh");
                        //description.setOutputFiles("output.README");                    
                        // Submit asynchronously
                        
                        // Following function needs a new GE Version having
                        // a boolean field at the bottom of its argument list
                        // Setting to true the function will return tha 
                        // corresponding job' ActiveGridInteracion value
                        // The current workaround leaved agi_id = 0 then
                        // the controller daemon will update the correct id
                        // querying the ActiveGridInteraction filtering by the
                        // jobDescription = 'task_id: <#task_id>'
                        agi_id = 0;
                                   mijs.submitJobAsync(geCommonName
                                                      ,gedIPAddress
                                                      ,geAppId
                                                      ,jobIdentifier);  
                        _log.info("AGI_id: "+agi_id);
                    } catch (Exception e) {
                        _log.severe("Caught exception:"+LS+e.toString());
                    }
                    break;
                default:
                    _log.severe("Unrecognized or unsupported adaptor found!");
            }   
        } catch(IOException e) {
            _log.severe("Unable to load GridEngine JSON job description\n"+LS
                       +e.toString()); 
        } catch(Exception e) {
            _log.severe("Unable to submit job: "+LS+e.toString());
        }            
        return agi_id;
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public String jobStatus() {
        String jobStatus = null;
        GridEngineInterfaceDB geiDB = null;
        _log.info("Getting job status");
        // It is more convenient to directly query the ActiveGridInteraction
        // since GridEngine JobCheck threads are in charge to update this
        try {
            geiDB = new GridEngineInterfaceDB(utdb_host
                                             ,utdb_port
                                             ,utdb_user
                                             ,utdb_pass
                                             ,utdb_name);
            jobStatus = geiDB.getJobStatus(gedCommand.getAGIId());
        } catch (Exception e) {
            _log.severe("Unable get command status:"+LS+gedCommand
                                                    +LS+e.toString());
        }
        finally {
           if(geiDB!=null) geiDB.close(); 
        }                
        return jobStatus;
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

    /**
     * Return a JSON object containing information stored in file:
     * <action_info>/<task_id>.info file, which contains the job
     * description built for the GridEngine
     */    
    private JSONObject loadJSONJobDesc() throws IOException {
        JSONObject jsonJobDesc = null;
        
        String jobDescFileName = gedCommand.getActionInfo()+"/"
                                +gedCommand.getTaskId()
                                +".info";  
        try {
            InputStream is = new FileInputStream(jobDescFileName);
            String jsonTxt = IOUtils.toString(is);
            jsonJobDesc = (JSONObject) new JSONObject(jsonTxt);                        
            _log.info("Loaded GridEngine JobDesc:\n"+LS+jsonJobDesc);
        } catch(Exception e) {
            _log.info("Caught exception: "+ e.toString());
        }
        return jsonJobDesc;
    } 
    
    /**
     * Retrieve the id field of the ActiveGridInteraction table starting from
     * the jobDesc table
     * @param task_id
     */
    public int getAGIId() {
        int agi_id = 0;
        GridEngineInterfaceDB geiDB = null;
        _log.info("Getting ActiveGridInteraciton' id field for task: "
                 +gedCommand.getTaskId());
        try {
            geiDB = new GridEngineInterfaceDB(utdb_host
                                             ,utdb_port
                                             ,utdb_user
                                             ,utdb_pass
                                             ,utdb_name);
            agi_id = geiDB.getAGIId(gedCommand.getTaskId());
        } catch (Exception e) {
            _log.severe("Unable get id:"+LS+gedCommand
                                                    +LS+e.toString());
        }
        finally {
           if(geiDB!=null) geiDB.close(); 
        }               
        return agi_id;
    }
}
