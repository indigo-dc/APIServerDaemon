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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
//import java.util.logging.Logger;
import org.apache.log4j.Logger;

/**
 * Class that contain all APIServerDaemon configuration settings
 * It manages a configuration file and/or static settings
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see APIServerDaemon
 */
public class APIServerDaemonConfig {
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(APIServerDaemonConfig.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    /*
      Configuration file
    */
    private final String asdPropetiesFile = "APIServerDaemon.properties";
    /*
       Database settings
     */        
    private String apisrv_dbhost = "localhost";
    private String apisrv_dbport = "3306";
    private String apisrv_dbuser = "fgapiserver";
    private String apisrv_dbpass = "fgapiserver_password";
    private String apisrv_dbname = "fgapiserver";
    private String apisrv_dbver  = "";
    
    /*
      GrigEngineDaemon settings
    */
    private int asdMaxThreads   =  100;
    private int asdCloseTimeout =   20;
    
    /*
      GridEngineDaemonPolling settings
    */
    private int asPollingDelay       = 4000;
    private int asPollingMaxCommands =    5;
    
    /*
      GridEngineDaemonController settings
    */
    private int asControllerDelay       =10000;
    private int asControllerMaxCommands =    5;
    
    /*
      GridEngineDaemon task retry policies
    */
    private int asTaskMaxRetries = 5;
    private int asTaskMaxWait = 1800000; // 30*60*1000 (30 min in milliseconds)
    
    /*
     GridEngine UsersTracking DB
    */  
    private String utdb_jndi = "jdbc/UserTrackingPool";
    private String utdb_host = "localhost";
    private String utdb_port = "3306";
    private String utdb_user = "tracking_user";
    private String utdb_pass = "usertracking";
    private String utdb_name = "userstracking";             
    
    /**
     * Load the given configuration file which overrides static settings
     * @param showConf - when true shows loaded configuration parameters      
     */
    public APIServerDaemonConfig(boolean showConf) { 
        /*
          Load a configuration file containing APIServerDaemon settings
          wich override the static settings defined in the class
        */    
        loadProperties();
        if(showConf)
            _log.info("APIServerDaemon config:"+LS+this.toString());        
    }
    
    /**
     * Load APIServerDaemon.properties values
     */
    private void loadProperties() {
        InputStream inputStream=null;        
        Properties prop = new Properties();
        try {
            inputStream = this.getClass().
                    getResourceAsStream(asdPropetiesFile);            
            
            prop.load(inputStream);
            
            /*
              Retrieving configuration values 
            */
            
            // APIServer DB settings 
            String prop_apisrv_dbhost = prop.getProperty("apisrv_dbhost");
            String prop_apisrv_dbport = prop.getProperty("apisrv_dport");
            String prop_apisrv_dbuser = prop.getProperty("apisrv_dbuser");
            String prop_apisrv_dbpass = prop.getProperty("apisrv_dbpass");
            String prop_apisrv_dbname = prop.getProperty("apisrv_dbname");
            String prop_apisrv_dbver  = prop.getProperty("apisrv_dbver");
                    
            // GridEngineDaemon thread settings
            String prop_asdMaxThreads = prop.getProperty("asdMaxThreads");
            String prop_asdCloseTimeout = prop.getProperty("asdCloseTimeout");
                    
            // GridEngineDaemonPolling settings
            String prop_asPollingDelay = prop.getProperty("asPollingDelay");
            String prop_asPollingMaxCommands = prop.getProperty("asPollingMaxCommands");
            
            // GridEngineDaemonController settings
            String prop_asControllerDelay = prop.getProperty("asControllerDelay");
            String prop_asControllerMaxCommands = prop.getProperty("asControllerMaxCommands");
            
            // GridEngineDaemon retry policies
            String prop_asTaskMaxRetries = prop.getProperty("asTaskMaxRetries");
            String prop_asTaskMaxWait = prop.getProperty("asTaskMaxWait");
                        
            // GridEngine' UsersTracking database settings
            String prop_utdb_jndi = prop.getProperty("utdb_jndi");
            String prop_utdb_host = prop.getProperty("utdb_host");
            String prop_utdb_port = prop.getProperty("utdb_port");            
            String prop_utdb_user = prop.getProperty("utdb_user");
            String prop_utdb_pass = prop.getProperty("utdb_pass");
            String prop_utdb_name = prop.getProperty("utdb_name");  
            
            /*
              Override or use class' settings
            */
            
            // APIServer DB settings 
            if(prop_apisrv_dbhost!=null) this.apisrv_dbhost=prop_apisrv_dbhost;
            if(prop_apisrv_dbport!=null) this.apisrv_dbport=prop_apisrv_dbport;
            if(prop_apisrv_dbuser!=null) this.apisrv_dbuser=prop_apisrv_dbuser;
            if(prop_apisrv_dbpass!=null) this.apisrv_dbpass=prop_apisrv_dbpass;
            if(prop_apisrv_dbname!=null) this.apisrv_dbname=prop_apisrv_dbname;
            if(prop_apisrv_dbver !=null) this.apisrv_dbname=prop_apisrv_dbver;
            
            // APIServerDaemon thread settings
            if(prop_asdMaxThreads!=null) 
                this.asdMaxThreads = Integer.parseInt(prop_asdMaxThreads);
            if(prop_asdCloseTimeout!=null) 
                this.asdCloseTimeout=Integer.parseInt(prop_asdCloseTimeout);
                    
            // APIServerDaemonPolling settings
            if(prop_asPollingDelay!=null) 
                this.asPollingDelay=Integer.parseInt(prop_asPollingDelay);
            if(prop_asPollingMaxCommands!=null) 
                this.asPollingMaxCommands=Integer.parseInt(prop_asPollingMaxCommands);
            
            // APIServerDaemonController settings
            if(prop_asControllerDelay!=null)
                this.asControllerDelay=Integer.parseInt(prop_asControllerDelay);
            if(prop_asControllerMaxCommands!=null)
                this.asControllerMaxCommands=Integer.parseInt(prop_asControllerMaxCommands);
            
            // APIServerDaemon task retry policies
            if(prop_asTaskMaxRetries!=null)
                this.asTaskMaxRetries=Integer.parseInt(prop_asTaskMaxRetries);
            if(prop_asTaskMaxWait!=null)
                this.asTaskMaxWait=Integer.parseInt(prop_asTaskMaxWait);            
            
            // GridEngine' UsersTracking database settings
            if(prop_utdb_jndi!=null)
                this.utdb_jndi=prop_utdb_jndi;
            if(prop_utdb_host!=null)
                this.utdb_host=prop_utdb_host;
            if(prop_utdb_port!=null)
                this.utdb_port=prop_utdb_port;
            if(prop_utdb_user!=null)
                this.utdb_user=prop_utdb_user;
            if(prop_utdb_pass!=null)
                this.utdb_pass=prop_utdb_pass;
            if(prop_utdb_name!=null)
                this.utdb_name=prop_utdb_name;
        } catch(NullPointerException e) {          
            _log.warn("Unable to load property file; using default settings");
        } catch(IOException e) {
            _log.warn("Error reading file: "+ e);
        } catch(NumberFormatException e) {          
            _log.warn("Error while reading property file: "+ e);
        }
        finally {
                try {
                    if(null != inputStream) inputStream.close();
                } catch (IOException e) {
                        System.out.println("Error closing configuration file input stream");
                }
        }
}
    
    /*
      Get and set methods ...
    */

    /*
      Database settings
    */
    
    /**
     * Prepare a connectionURL from detailed connection settings
     */
    public String getApisrv_URL() {
        String APIServerConnURL = "jdbc:mysql://" + getApisrv_dbhost()
                                + ":"             + getApisrv_dbport() 
                                + "/"             + getApisrv_dbname() 
                                + "?user="        + getApisrv_dbuser() 
                                + "&password="    + getApisrv_dbpass();
        _log.debug("APIServerDB ConnectionURL: '"+APIServerConnURL+"'");
        return APIServerConnURL;
    }
    
    /**
     * Get APIServerDaemon database name
     * @return apisrv_dbname
     */
    public String getApisrv_dbname() {
        return apisrv_dbname;
    }
    /**
     * Set APIServer database name
     * @param apisrv_dbname 
     */
    public void setApisrv_dbname(String apisrv_dbname) {
        this.apisrv_dbname = apisrv_dbname;
    }

    /**
     * Get APIServerDaemon database host
     * @return apisrv_dbname
     */
    public String getApisrv_dbhost() {
        return apisrv_dbhost;
    }
    /**
     * Set APIServer database host
     * @param apisrv_dbhost
     */
    public void setApisrv_dbhost(String apisrv_dbhost) {
        this.apisrv_dbhost = apisrv_dbhost;
    }

    /**
     * Get APIServer database port
     * @return apisrv_dbport
     */
    public String getApisrv_dbport() {
        return apisrv_dbport;
    }
    /**
     * Set APIServer database port
     * @param apisrv_dbport
     */
    public void setApisrv_dbport(String apisrv_dbport) {
        this.apisrv_dbport = apisrv_dbport;
    }

    /**
     * Get APIServer database user
     * @return apisrv_dbuser
     */
    public String getApisrv_dbuser() {
        return apisrv_dbuser;
    }
    /**
     * Set APIServer database user
     * @param apisrv_dbuser
     */
    public void setApisrv_dbuser(String apisrv_dbuser) {
        this.apisrv_dbuser = apisrv_dbuser;
    }

    /**
     * Get APIServer database password
     * @return apisrv_pass
     */
    public String getApisrv_dbpass() {
        return apisrv_dbpass;
    }
    /**
     * Set APIServer database password
     * @param apisrv_dbpass
     */
    public void setApisrv_dbpass(String apisrv_dbpass) {
        this.apisrv_dbpass = apisrv_dbpass;
    }

    /**
     * Get APIServerDaemon thread closure timeout
     * @return asdCloseTimeout number of seconds waiting for thread closure
     */public int getASDCloseTimeout() {
        return asdCloseTimeout;
    }
    /**
     * Set APIServerDaemon thread closure timeout
     * @param asdCloseTimeout 
     */
    public void setASDCloseTimeout(int asdCloseTimeout) {
        this.asdCloseTimeout = asdCloseTimeout;
    }
    /**
     * Get APIServerDaemon database version
     * @return apisrv_dbname
     */
    public String getApisrv_dbver() {
        return apisrv_dbver;
    }
    /**
     * Set APIServer database version
     * @param apisrv_dbver
     */
    public void setApisrv_dbver(String apisrv_dbver) {
        this.apisrv_dbver = apisrv_dbver;
    }
    
        
    /*
      APIServerDaemon
    */
    
    /**
     * Get APIServerDaemon max number of threads
     * @return asdMaxThreads maximum number of threads
     */
    public int getMaxThreads() { 
        return asdMaxThreads; 
    }
    /**
     * Set APIServerDaemon max number of threads
     * @param gedMaxThreads maximum number of threads
     */
    public void setMaxThreads(int gedMaxThreads) { 
        this.asdMaxThreads=gedMaxThreads; 
    }
    
    /**
     * Get APIServerDaemon closing thread timeout value
     * @return asdMaxThreads maximum number of threads
     */
    public int getCloseTimeout() { 
        return asdCloseTimeout; 
    }
    /**
     * Set APIServerDaemon closing thread timeout value
     * @param asdCloseTimeout closing thread timeout value
     */
    public void setCloseTimeout(int asdCloseTimeout) { 
        this.asdCloseTimeout=asdCloseTimeout; 
    }
    
    /*
      APIServerDaemonPolling settings
    */  
    
    /**
     * Get polling thread loop delay
     * @return asPollingDelay number of seconds for each controller loop
     */
    public int getPollingDelay() {
        return asPollingDelay;
    }
    /**
     * Set polling thread loop delay
     * @param asPollingDelay 
     */
    public void setPollingDelay(int asPollingDelay) {
        this.asPollingDelay = asPollingDelay;
    }
    /**
     * Get polling thread max number of commands per loop
     * @return asPollingMaxCommands number of records to be extracted 
 from ge_queue table
     */
    public int getPollingMaxCommands() {
        return asPollingMaxCommands;
    }
    /**
     * Set polling thread max number of commands per loop
     * @param asPollingMaxCommands 
     */
    public void setPollingMaxCommands(int asPollingMaxCommands) {
        this.asPollingMaxCommands = asPollingMaxCommands;
    }

    /*
      APIServerDaemonController settings
    */  
    
    /**
     * Get controller thread loop delay
     * @return asControllerDelay number of seconds for each controller loop
     */
    public int getControllerDelay() {
        return asControllerDelay;
    }
    /**
     * Set controller thread loop delay
     * @param asControllerDelay 
     */
    public void setControllerDelay(int asControllerDelay) {
        this.asControllerDelay = asControllerDelay;
    }
    /**
     * Get controller thread max number of commands per loop
     * @return asControllerMaxCommands number of records to be extracted 
     * from ge_queue table
     */
    public int getControllerMaxCommands() {
        return asControllerMaxCommands;
    }
    /**
     * Set controller thread max number of commands per loop
     * @param asControllerMaxCommands 
     */
    public void setControllerMaxCommands(int asControllerMaxCommands) {
        this.asControllerMaxCommands = asControllerMaxCommands;
    }
    
    /**
     * GridEngine jndi database resource
     * @return usertracking jndi resource name
     */
    String getGridEngine_db_jndi() { return this.utdb_jndi; }
    
    /**
     * GridEngine jdni database resource
     * @param usertracking jndi  resource name
     */
    void setGridEngine_db_jndi(String utdb_jndi) { this.utdb_jndi=utdb_jndi; }     
    /**
     * Return the GridEngine' userstracking database host
     * @return userstracking database host
     */
    String getGridEngine_db_host() { return this.utdb_host; }
    /**
     * Set the GridEngine' userstracking database host
     * @param userstracking database host
     */
    void setGridEngine_db_host(String utdb_host) { this.utdb_host=utdb_host; } 
    /**
     * Return the GridEngine' userstracking database port
     * @return userstracking database port
     */
    String getGridEngine_db_port() { return this.utdb_port; }
    /**
     * Set the GridEngine' userstracking database port
     * @param userstracking database port
     */
    void setGridEngine_db_port(String utdb_port) { this.utdb_port=utdb_port; } 
    /**
     * Return the GridEngine' userstracking database user
     * @return userstracking database user
     */
    String getGridEngine_db_user() { return this.utdb_user; }
    /**
     * Set the GridEngine' userstracking database user
     * @param userstracking database user
     */
    void setGridEngine_db_user(String utdb_user) { this.utdb_user=utdb_user; } 
    /**
     * Return the GridEngine' userstracking database password
     * @return userstracking database password
     */
    String getGridEngine_db_pass() { return this.utdb_pass; }
    /**
     * Set the GridEngine' userstracking database password
     * @param userstracking database password
     */
    void setGridEngine_db_pass(String utdb_pass) { this.utdb_pass=utdb_pass; } 
    /**
     * Return the GridEngine' userstracking database name
     * @return userstracking database name
     */
    String getGridEngine_db_name() { return this.utdb_name; }
    /**
     * Set the GridEngine' userstracking database name
     * @param userstracking database name
     */
    void setGridEngine_db_name(String utdb_name) { this.utdb_name=utdb_name; } 
    
    /*
      APIServerDaemon task retry policies
    */
    
    /**
     * Return the maximum number of retries for a task request
     */
    int getTaskMaxRetries() { return this.asTaskMaxRetries; }
    /**
     * Return maximum number of seconds before to try a task retry
     */
    int getTaskMaxWait() { return this.asTaskMaxWait; }    
    /**
     * Set the maximum number of retries for a task request
     * @param maximum number of retries for a task request
     */
    void setTaskMaxRetries(int maxRetries) { this.asTaskMaxRetries=maxRetries; } 
    /**
     * Set the maximum number of seconds before to try a task retry
     * @param maximum number of seconds before to try a task retry
     */
    void setTaskMaxWait(int maxWait) { this.asTaskMaxWait=maxWait; }         
    
    /**
     * View configuration settings
     */
    @Override
    public String toString() {
        /*
          Database settings
        */
        return "[API Server DB settings]"                                   +LS              
              +"    db_host : '"                +apisrv_dbhost          +"'"+LS
              +"    db_port : '"                +apisrv_dbport          +"'"+LS
              +"    db_user : '"                +apisrv_dbuser          +"'"+LS        
              +"    db_pass : '"                +apisrv_dbpass          +"'"+LS        
              +"    db_name : '"                +apisrv_dbname          +"'"+LS
              +"[APIServerDaemon settings]"                            +"'"+LS
              +"    asdMaxThreads   : '"        +asdMaxThreads          +"'"+LS
              +"    asdCloseTimeout : '"        +asdCloseTimeout        +"'"+LS
              +"[APIServerDaemonPolling settings]"                     +"'"+LS
              +"    asPollingDelay       : '"   +asPollingDelay         +"'"+LS
              +"    asPollingMaxCommands : '"   +asPollingMaxCommands   +"'"+LS 
              +"[APIServerDaemonController settings]"                  +"'"+LS
              +"    asControllerDelay       : '"+asControllerDelay      +"'"+LS
              +"    asControllerMaxCommands : '"+asControllerMaxCommands+"'"+LS
              +"[APIServerDaemon task retry policies]"                      +LS
              +"    asTaskMaxRetries  : '"      +asTaskMaxRetries       +"'"+LS
              +"    asTaskMaxWait     : '"      +asTaskMaxWait          +"'"+LS  
              +"[GridEngine UsersTracking DB settings]"                     +LS
              +"    db_jndi : '"                +utdb_jndi              +"'"+LS
              +"    db_host : '"                +utdb_host              +"'"+LS
              +"    db_port : '"                +utdb_port              +"'"+LS
              +"    db_user : '"                +utdb_user              +"'"+LS        
              +"    db_pass : '"                +utdb_pass              +"'"+LS        
              +"    db_name : '"                +utdb_name              +"'"+LS;                   
    }
}
