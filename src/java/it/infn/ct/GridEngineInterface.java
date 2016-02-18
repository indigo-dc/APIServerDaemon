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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;
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
    APIServerDaemonConfig gedConfig;
    /*
      GridEngineDaemon IP address
    */
    String gedIPAddress;
    /*
      Logger
    */  
    private static final Logger _log = Logger.getLogger(GridEngineInterface.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    APIServerDaemonCommand gedCommand;
    
    /**
     * Empty constructor for GridEngineInterface
     */
    public GridEngineInterface() {
        _log.debug("Initializing GridEngineInterface");
        // Retrieve host IP address, used by JobSubmission
        getIP();
        // Prepare environment variable for GridEngineLogConfig.xml
        setupGELogConfig();
    }
    
    /**
     * Constructor for GridEngineInterface taking as input a given command
     */
    public GridEngineInterface(APIServerDaemonCommand gedCommand) {
        this();
        _log.debug("GridEngineInterface command:"+LS+gedCommand);
        this.gedCommand=gedCommand;        
    }
    /**
     * Constructor for GridEngineInterface taking as input the
     * APIServerDaemonConfig and a given command
     */
    public GridEngineInterface(APIServerDaemonConfig gedConfig
                              ,APIServerDaemonCommand gedCommand) {
        this();
        _log.debug("GridEngineInterface command:"+LS+gedCommand);
        setConfig(gedConfig);       
        this.gedCommand=gedCommand;        
    }
    
    /*
      GridEngine interfacing methods
    */
    
    /**
     * Load GridEngineDaemon configuration settings
     * @param gedConfig GridEngineDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig gedConfig) {
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
     * Retrieve the APIServerDaemon PATH to the GridEngineLogConfig.xml file
     * and setup the GridEngineLogConfig.path environment variable accordingly
     * This variable will be taken by GridEngine while building up its log
     */
    private void setupGELogConfig() {
        URL GELogConfig = this.getClass().getResource("GridEngineLogConfig.xml");
        String GELogConfigEnvVar=GELogConfig.getPath();
        _log.debug("GridEngineLogConfig.xml at '"+GELogConfigEnvVar+"'");
        Properties props = System.getProperties();
        props.setProperty("GridEngineLogConfig.path", GELogConfigEnvVar);
        System.setProperties(props);
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
        // InputSandbox
        String inputSandbox = "";
        _log.debug("Input files:");
        for(int i=0; i<input_files.length(); i++) {               
            JSONObject input_entry = input_files.getJSONObject(i);            
            if (input_entry.getString("name").length() > 0) {
                String comma=(i==0)?"":",";
                inputSandbox += comma
                               +gedCommand.getActionInfo()+"/"
                               +input_entry.getString("name");
            }
        }
        mijs.setInputFiles(inputSandbox);
        _log.debug("inputSandbox: '"+inputSandbox+"'");
        
        // OutputSandbox
        String outputSandbox = "";
        _log.debug("Output files:");
        for(int i=0; i<output_files.length(); i++) {            
            JSONObject output_entry = output_files.getJSONObject(i);
            if (output_entry.getString("name").length() > 0) {
                String comma=(i==0)?"":",";
                outputSandbox += comma
                                +output_entry.getString("name");                
            }
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
            _log.error("mijs is NULL, sorry!");       
        else try {            
            _log.debug("Loading GridEngine job JSON desc");
            // Load <task_id>.json file in memory
            JSONObject geJobDesc=mkGEJobDesc(); 
            // application
            int geAppId = geJobDesc.getInt("application");
            // commonName (user executing task)
            String geCommonName = geJobDesc.getString("commonName");
            // infrastructure
            JSONObject geInfrastructure = 
                    geJobDesc.getJSONObject("infrastructure");
            // jobDescription
            JSONObject geJobDescription = 
                    geJobDesc.getJSONObject("jobDescription");
            // credentials
            JSONObject geCredentials = 
                    geJobDesc.getJSONObject("credentials");
            // identifier
            String jobIdentifier =
                    geJobDesc.getString("identifier");
            // input_files
            JSONArray input_files = 
                    geJobDesc.getJSONArray("input_files");
            // output_files
            JSONArray output_files = 
                    geJobDesc.getJSONArray("output_files");
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
                {
                    try {
                        _log.info("Entering SSH adaptor ...");
                        // Credential values
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
                }
                break;
                // rOCCI Adaptor
                case "rocci": 
                {
                    _log.info("Entering rOCCI adaptor ...");
                    
                    // Infrastructure values
                    String os_tpl=geInfrastructure.getString("os_tpl");
                    String resource_tpl=geInfrastructure.getString("resource_tpl");
                    String attributes_title=geInfrastructure.getString("attributes_title");                    
                    
                    // Credential values
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
                            resourceManagers   // Infrastruture name
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
                }
                break;
                // wms adaptor (EMI/gLite)
                case "wms":
                {
                    _log.info("Entering wms adaptor ...");
                    
                    // Infrastructure values
                    String bdii = geInfrastructure.getString("bdii");
                    // jdlRequirements and swtags are not mandatory
                    // catch JSONException exception if these values
                    // are missing
                    String jdlRequirements[] = null;
                    try {
                        jdlRequirements = geInfrastructure.getString("jdlRequirements").split(";");                        
                    } catch (JSONException e) {                        
                        _log.info("jdlRequirements not specified");
                    }
                    String swtags = null;
                    try {                        
                        swtags = geInfrastructure.getString("swtags");
                    } catch (JSONException e) {                        
                        _log.info("swtags not specified");
                    }
                    // Credentials values
                    String eToken_host = geCredentials.getString("eToken_host");
                    String eToken_port = geCredentials.getString("eToken_port");
                    String eToken_id = geCredentials.getString("eToken_id");
                    String voms = geCredentials.getString("voms");
                    String voms_role = geCredentials.getString("voms_role");
                    String rfc_proxy = geCredentials.getString("rfc_proxy");
                                        
                    String wmsList[] = { resourceManagers };                    
                    infrastructures[0] = new 
                        InfrastructureInfo( 
                            resourceManagers         // Infrastruture name
                           ,"wms"                    // Adaptor
                           ,wmsList                  //                           
                           ,eToken_host              // eTokenServer host
                           ,eToken_port              // eTokenServer port
                           ,eToken_id                // eToken id (md5sum)
                           ,voms                     // VO
                           ,voms_role                // VO.group.role
                           ,(null!=swtags)?swtags:"" // Software Tags
                        );
                    mijs.addInfrastructure(infrastructures[0]);                    
                    // Setup JobDescription
                    prepareJobDescription(mijs,geJobDescription);
                    // I/O Sandbox
                    // In wms output and error files have to be removed
                    // from output_files
                    for(int i=0; i<output_files.length(); i++) {            
                        JSONObject output_entry = output_files.getJSONObject(i);
                        if (   output_entry.getString("name").equals(geJobDescription.getString("output"))
                            || output_entry.getString("name").equals(geJobDescription.getString("error"))) {
                            output_files.getJSONObject(i).put("name", "");
                            _log.debug("Skipping file: '"+output_entry.getString("name")+"'");
                        }
                    }
                    prepareIOSandbox(mijs,input_files,output_files);                    
                    // JDL requirements                    
                    if(jdlRequirements!=null && jdlRequirements.length > 0)
                        mijs.setJDLRequirements(jdlRequirements);                   
                    // Submit asynchronously                        
                    agi_id = 0;
                               mijs.submitJobAsync(geCommonName
                                                  ,gedIPAddress
                                                  ,geAppId
                                                  ,jobIdentifier);  
                    _log.debug("AGI_id: "+agi_id);                     
                }
                break;
                default:                  
                    _log.fatal("Unrecognized or unsupported adaptor found!");
            }   
        } catch(IOException e) {          
            _log.fatal("Unable to load APIServer JSON job description\n"+LS
                       +e.toString()); 
        }
        catch(Exception e) {          
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
            jobStatus = geiDB.getJobStatus(gedCommand.getTargetId());
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
     * <action_info>/<task_id>.json file, which contains the job
     * description built by the APIServer translated for the 
     * GridEngine
     */    
    private JSONObject mkGEJobDesc() throws IOException {
        JSONObject jsonJobDesc = null;
        
        _log.debug("Entering mkGEJobDesc");
        
        String jobDescFileName = gedCommand.getActionInfo()+"/"
                                +gedCommand.getTaskId()
                                +".json"; 
        _log.debug("JSON filename: "+jobDescFileName);
        try {
            InputStream is = new FileInputStream(jobDescFileName);
            String jsonTxt = IOUtils.toString(is);            
            jsonJobDesc = (JSONObject) new JSONObject(jsonTxt);            
            _log.debug("Loaded APIServer JobDesc:\n"+LS+jsonJobDesc);
        } catch(Exception e) {
            _log.warn("Caught exception: "+ e.toString());
        }
        
        // Now create the <task_id>.info file targeted for the GridEngine
        JSONObject GridEngineTaskDescription = new JSONObject();
        GridEngineTaskDescription.put("commonName",String.format("%s", jsonJobDesc.getString("user")));
        GridEngineTaskDescription.put("application",10000); // Take this value from properties file or any other configuration source
        GridEngineTaskDescription.put("identifier",String.format("%s@%s",jsonJobDesc.getString("id")
                                                                        ,jsonJobDesc.getString("iosandbox"))); 
        GridEngineTaskDescription.put("input_files",jsonJobDesc.getJSONArray("input_files"));
        GridEngineTaskDescription.put("output_files",jsonJobDesc.getJSONArray("output_files"));
                
        // Prepare the JobDescription
        JSONObject GridEgnineJobDescription = new JSONObject();
        
        // Get app Info and Parameters
        JSONObject appInfo = new JSONObject();
        appInfo = jsonJobDesc.getJSONObject("application");
        JSONArray appParams = new JSONArray();
        appParams = appInfo.getJSONArray("parameters");
                
        // Process application parameters
        String job_args="";
        String param_name;
        String param_value;
        for(int i=0; i<appParams.length(); i++) {               
            JSONObject appParameter = appParams.getJSONObject(i);
            
            // Get parameter name and value
            param_name  = appParameter.getString("param_name");
            param_value = appParameter.getString("param_value");           
            
            // Map task values to GE job description values
            if(param_name.equals("jobdesc_executable"))
                GridEgnineJobDescription.put("executable", param_value);
            else if(param_name.equals("jobdesc_arguments")) {
              // Further arguments will be added later              
              job_args=param_value+" ";
            }
            else if(param_name.equals("jobdesc_output"))
                GridEgnineJobDescription.put("output", param_value);
            else if(param_name.equals("jobdesc_error"))
                GridEgnineJobDescription.put("error", param_value);
            else {
                _log.warn("Reached end of if-elif chain for param name: '"
                        +param_name+"' with value: '"
                        +param_value+"'");
            }
        }
        
        // Now add further arguments if specified in task
        JSONArray jobArguments = jsonJobDesc.getJSONArray("arguments");       
        for(int j=0; j<jobArguments.length(); j++)
            job_args += String.format("%s ",jobArguments.getString(j));        
        GridEgnineJobDescription.put("arguments", job_args.trim());
                            
        // Get application specific settings
        GridEngineTaskDescription.put("jobDescription", GridEgnineJobDescription);
                
        // Select one of the possible infrastructures among the one enabled
        // A random strategy is currently implemented; this could be changed later        
        JSONArray jobInfrastructures = appInfo.getJSONArray("infrastructures");
        JSONArray enabledInfras = new JSONArray();
        for(int v=0,w=0; w<jobInfrastructures.length(); w++) {
            JSONObject infra = jobInfrastructures.getJSONObject(w);
            if(infra.getString("status").equals("enabled"))
                enabledInfras.put(v++,infra);
        }
        int selInfraIdx = 0;
        Random rndGen = new Random();
        if(enabledInfras.length()>1) {
            selInfraIdx = rndGen.nextInt(enabledInfras.length());
        }
        JSONObject selInfra = new JSONObject();
        selInfra = enabledInfras.getJSONObject(selInfraIdx);
        _log.debug("Selected infra:"+LS+selInfra.toString(4));
                
        // Process infrastructure parameters
        JSONObject GridEngineInfrastructure = new JSONObject();
        JSONObject GridEngineCredentials = new JSONObject();
                
        JSONArray infraParams = selInfra.getJSONArray("parameters");        
        for(int h=0; h<infraParams.length(); h++) {
            JSONObject infraParameter = infraParams.getJSONObject(h);
            param_name = infraParameter.getString("name");
            param_value = infraParameter.getString("value");
            _log.info(h+": "+param_name+" - "+param_value);
            // Job settings
            if(param_name.equals("jobservice"))
                GridEngineInfrastructure.put("resourceManagers",param_value);
            else if(param_name.equals("os_tpl"))
                GridEngineInfrastructure.put("os_tpl",param_value);
            else if(param_name.equals("resource_tpl"))
                GridEngineInfrastructure.put("resource_tpl",param_value);
            else if(param_name.equals("attributes_title"))
                GridEngineInfrastructure.put("attributes_title",param_value);
            else if(param_name.equals("bdii"))
                GridEngineInfrastructure.put("bdii",param_value);
            else if(param_name.equals("swtags"))
                GridEngineInfrastructure.put("swtags",param_value);
            else if(param_name.equals("jdlRequirements"))
                GridEngineInfrastructure.put("jdlRequirements",param_value);
            // Credential settings
            else if(param_name.equals("username"))
                GridEngineCredentials.put("username",param_value);
            else if(param_name.equals("password"))
                GridEngineCredentials.put("password",param_value);
            else if(param_name.equals("eToken_host"))
                GridEngineCredentials.put("eToken_host",param_value);
            else if(param_name.equals("eToken_port"))
                GridEngineCredentials.put("eToken_port",param_value);
            else if(param_name.equals("eToken_id"))
                GridEngineCredentials.put("eToken_id",param_value);
            else if(param_name.equals("voms"))
                GridEngineCredentials.put("voms",param_value);
            else if(param_name.equals("voms_role"))
                GridEngineCredentials.put("voms_role",param_value);
            else if(param_name.equals("rfc_proxy"))
                GridEngineCredentials.put("rfc_proxy",param_value);
            else {
                _log.warn("Reached end of if-elif chain for infra_param name: '"
                        +param_name+"' with value: '"
                        +param_value+"'");
            }
        }         
        GridEngineTaskDescription.put("infrastructure",GridEngineInfrastructure);
        GridEngineTaskDescription.put("credentials",GridEngineCredentials);
        
        // Now write the JSON translated for the GridEngine 
        String JSONTask = GridEngineTaskDescription.toString();
        String JSONFileName = gedCommand.getActionInfo()+"/"
                             +gedCommand.getTaskId()
                             +".ge_info";  
        try {
            OutputStream os = new FileOutputStream(JSONFileName);
            os.write(JSONTask.getBytes(Charset.forName("UTF-8"))); // UTF-8 from properties            
            _log.debug("GridEngine JobDescription written in file '"+JSONFileName+"':\n"+LS+JSONTask);
        } catch(Exception e) {
            _log.warn("Caught exception: "+ e.toString());
        }
        
        return GridEngineTaskDescription;
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
                 +gedCommand.getTargetId());
        try {
            geiDB = new GridEngineInterfaceDB(utdb_host
                                             ,utdb_port
                                             ,utdb_user
                                             ,utdb_pass
                                             ,utdb_name);
            jobDesc = geiDB.getJobDescription(gedCommand.getTargetId());
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
                                                +gedCommand.getTargetId()+".tgz");
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
                                             +gedCommand.getTargetId());
    }
    
}
