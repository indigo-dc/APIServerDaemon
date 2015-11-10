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
import org.json.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * This class interfaces any call to the GridEngine library
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class GridEngineInterface {
    /*
     GridEngine UsersTracking DB
    */ 
    private String utdb_jndi;
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
    private static final Logger _log = Logger.getLogger(GridEngineInterface.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    GridEngineDaemonCommand gedCommand;
    
    /**
     * Empty constructor for GridEngineInterface
     */
    public GridEngineInterface() {
        _log.debug("Initializing GridEngineInterface");
        getIP();
    }
    /**
     * Constructor for GridEngineInterface taking as input a given command
     */
    public GridEngineInterface(GridEngineDaemonCommand gedCommand) {
        this();
        _log.debug("GridEngineInterface command:"+LS+gedCommand);
        this.gedCommand=gedCommand;        
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
        this.utdb_jndi = gedConfig.getGridEngine_db_jndi();
        this.utdb_host = gedConfig.getGridEngine_db_host();
        this.utdb_port = gedConfig.getGridEngine_db_port();
        this.utdb_user = gedConfig.getGridEngine_db_user();
        this.utdb_pass = gedConfig.getGridEngine_db_pass();
        this.utdb_name = gedConfig.getGridEngine_db_name();
        _log.debug(
                  "GridEngineInterface config:"            +LS
                 +"  [UsersTrackingDB]"                    +LS
                 +"    db_jndi: '"      +this.utdb_jndi+"'"+LS
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
            _log.fatal("Unable to get the portal IP address");
        }
    }
    
    /**
     * Prepares JobDescription specified in JSONObject item to setup
     * the given MultiInfrastructureJobSubmission object
     * @param MultiInfrastructureJobSubmission object instance
     * @param JSON Object describing the job description
     * @see MultiInfrastructureJobSubmission
     */
    private void prepareJobDescription(MultiInfrastructureJobSubmission mijs
                                      ,JSONObject geJobDescription) {
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
    }
    
    /**
     * Prepare the I/O Sandbox 
     */
    private void prepareIOSandbox(MultiInfrastructureJobSubmission mijs
                    ,JSONArray input_files
                    ,JSONArray output_files) {
        String inputSandbox = "";
        _log.debug("Input files:");
        for(int i=0; i<input_files.length(); i++) {   
            String comma=(i==0)?"":",";
            inputSandbox += comma
                           +gedCommand.getActionInfo()+"/"
                           +input_files.getString(i);
            _log.debug(gedCommand.getActionInfo()+"/"
                      +input_files.getString(i));
        }
        mijs.setInputFiles(inputSandbox);
        _log.debug("inputSandbox: '"+inputSandbox+"'");
        String outputSandbox = "";
        _log.debug("Output files:");
        for(int i=0; i<output_files.length(); i++) {
            String comma=(i==0)?"":",";
            JSONObject output_entry = output_files.getJSONObject(i);
            outputSandbox += comma
                            +output_entry.getString("name");
            _log.debug(output_entry.getString("name"));
        }
        mijs.setOutputFiles(outputSandbox);
        _log.debug("outputSandbox: '"+outputSandbox+"'");
    } 
    
    /**
     * submit the job identified by the gedCommand values
     * @return 
     */
    public int jobSubmit() {
        int agi_id=0;        
        _log.debug("Submitting job");
        // MultiInfrastructureJobSubmission object
        MultiInfrastructureJobSubmission mijs = null;
        if(utdb_jndi != null && ! utdb_jndi.isEmpty())
            mijs = new MultiInfrastructureJobSubmission();
        else
            mijs = new MultiInfrastructureJobSubmission(
                        "jdbc:mysql://"+utdb_host+":"
                                       +utdb_port+"/"
                                       +utdb_name
                        ,utdb_user
                        ,utdb_pass
                );
        if(mijs==null)
            _log.debug("mijs is NULL, sorry!");       
        else try {            
            _log.debug("Loading GridEngine job JSON desc");
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
            // input_files
            JSONArray input_files = 
                    jsonJobDesc.getJSONArray("input_files");
            // output_files
            JSONArray output_files = 
                    jsonJobDesc.getJSONArray("output_files");
            // Loaded essential JSON components; now go through
            // each adaptor specific setting:
            // resourceManagers
            String  resourceManagers = 
                    geInfrastructure.getString("resourceManagers");
            String adaptor = resourceManagers.split(":")[0];
            _log.info("Adaptor is '"+adaptor+"'");
            InfrastructureInfo infrastructures[] = new InfrastructureInfo[1];
            /*
              Each adaptor has its own specific settings
              Different adaptors may have in common some settings
              such as I/O Sandboxing, job description etc
            */
            switch(adaptor) {
                // SSH Adaptor
                case "ssh": 
                    try {
                        _log.info("Entering SSH adaptor ...");
                        String  username = 
                            geCredentials.getString("username");
                        String  password = 
                            geCredentials.getString("password");
                        String sshEndPoint[] = { resourceManagers };
                        infrastructures[0] = new InfrastructureInfo(
                                 resourceManagers
                               , "ssh"
                               , username
                               , password
                               , sshEndPoint);
                        mijs.addInfrastructure(infrastructures[0]);                    
                        // Job description
                        prepareJobDescription(mijs,geJobDescription);
                        // IO Files
                        prepareIOSandbox(mijs,input_files,output_files);
                        // Submit asynchronously                        
                        agi_id = 0;
                                   mijs.submitJobAsync(geCommonName
                                                      ,gedIPAddress
                                                      ,geAppId
                                                      ,jobIdentifier);  
                        _log.debug("AGI_id: "+agi_id);
                    } catch (Exception e) {                      
                        _log.fatal("Caught exception:"+LS+e.toString());
                    }
                    break;
                // rOCCI Adaptor
                case "rocci":
                    _log.info("Entering rOCCI adaptor ...");
                    String os_tpl=geInfrastructure.getString("os_tpl");
                    String resource_tpl=geInfrastructure.getString("resource_tpl");
                    String attributes_title=geInfrastructure.getString("attributes_title");                    
                    String eToken_host = geCredentials.getString("eToken_host");
                    String eToken_port = geCredentials.getString("eToken_port");
                    String eToken_id = geCredentials.getString("eToken_id");
                    String voms = geCredentials.getString("voms");
                    String voms_role = geCredentials.getString("voms_role");
                    String rfc_proxy = geCredentials.getString("rfc_proxy");
                    // Generate the rOCCI endpoint
                    String rOCCIResourcesList[] = {
                        resourceManagers+"/?"                       
                       +"action=create&"
                       +"resource=compute&"
                       +"mixin_resource_tpl="+resource_tpl+"&"
                       +"mixin_os_tpl="+os_tpl+"&"
                       +"attributes_title="+attributes_title+"&"
                       +"auth=x509"
                    };
                    _log.info("rOCCI endpoint: '"+rOCCIResourcesList[0]+"'");
                    // Prepare the infrastructure
                    infrastructures[0] = new 
                        InfrastructureInfo( 
                            "GE_rOCCI"         // Infrastruture name
                           ,"rocci"            // Adaptor
                           ,""                 //
                           ,rOCCIResourcesList // Resources list
                           ,eToken_host        // eTokenServer host
                           ,eToken_port        // eTokenServer port
                           ,eToken_id          // eToken id (md5sum)
                           ,voms               // VO
                           ,voms_role          // VO.group.role
                           ,rfc_proxy.equalsIgnoreCase("true") // ProxyRFC
                        );
                    mijs.addInfrastructure(infrastructures[0]);                    
                    // Setup JobDescription
                    prepareJobDescription(mijs,geJobDescription);
                    // I/O Sandbox                        
                    prepareIOSandbox(mijs,input_files,output_files);
                    // Submit asynchronously                        
                    agi_id = 0;
                               mijs.submitJobAsync(geCommonName
                                                  ,gedIPAddress
                                                  ,geAppId
                                                  ,jobIdentifier);  
                    _log.debug("AGI_id: "+agi_id);                    
                    break;
                default:                  
                    _log.fatal("Unrecognized or unsupported adaptor found!");
            }   
        } catch(IOException e) {          
            _log.fatal("Unable to load GridEngine JSON job description\n"+LS
                       +e.toString()); 
        } catch(Exception e) {          
            _log.fatal("Unable to submit job: "+LS+e.toString());
        }            
        return agi_id;
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public String jobStatus() {
        String jobStatus = null;
        GridEngineInterfaceDB geiDB = null;
        _log.debug("Getting job status");
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
            _log.fatal("Unable get command status:"+LS+gedCommand        
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
        _log.debug("Getting job output");
        return "NOTIMPLEMENTED";
    }
    /**
     * submit the job identified by the gedCommand values
     */
    public void jobCancel() {
        _log.debug("Cancelling job");
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
            _log.debug("Loaded GridEngine JobDesc:\n"+LS+jsonJobDesc);
        } catch(Exception e) {
            _log.warn("Caught exception: "+ e.toString());
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
        _log.debug("Getting ActiveGridInteraciton' id field for task: "
                 +gedCommand.getTaskId());
        try {
            geiDB = new GridEngineInterfaceDB(utdb_host
                                             ,utdb_port
                                             ,utdb_user
                                             ,utdb_pass
                                             ,utdb_name);
            agi_id = geiDB.getAGIId(gedCommand);
        } catch (Exception e) {          
            _log.fatal("Unable get id:"+LS+gedCommand
                                       +LS+e.toString());
        }
        finally {
           if(geiDB!=null) geiDB.close(); 
        }               
        return agi_id;
    }
    
    /**
     * Retrieve the id field of the ActiveGridInteraction table starting from
     * the jobDesc table
     * @param task_id
     */
    public String getJobDescription() {
        String jobDesc = "";
        GridEngineInterfaceDB geiDB = null;
        _log.debug("Getting jobDescription for AGI_id: "
                 +gedCommand.getAGIId());
        try {
            geiDB = new GridEngineInterfaceDB(utdb_host
                                             ,utdb_port
                                             ,utdb_user
                                             ,utdb_pass
                                             ,utdb_name);
            jobDesc = geiDB.getJobDescription(gedCommand.getAGIId());
        } catch (Exception e) {          
            _log.fatal("Unable get job description for command:"+LS+gedCommand
                                                                +LS+e.toString());
        }
        finally {
           if(geiDB!=null) geiDB.close(); 
        }               
        return jobDesc;
    }
    /**
     * Prepares the jobOuput for the APIServer
     * @return Directory containing output files
     */
    public String prepareJobOutput() { 
        String jobDescription = getJobDescription();
        String tgzFileName = 
                gedCommand.getActionInfo()+"/jobOutput/"
               +JSagaJobSubmission.
                       removeNotAllowedCharacter(jobDescription+"_"
                                                +gedCommand.getAGIId()+".tgz");
        _log.debug("tgzFileName: '"+tgzFileName+"'");
        try {
        Process unpackTar = 
                Runtime.getRuntime().exec("tar xzvf "
                                         +tgzFileName
                                         +" -C "
                                         +gedCommand.getActionInfo());
        unpackTar.waitFor();
        } catch (Exception e) {          
            _log.fatal("Error extracting archive: "+tgzFileName);
        }
        return JSagaJobSubmission.
                    removeNotAllowedCharacter(jobDescription+"_"
                                             +gedCommand.getAGIId());
    }
}
