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
import java.util.logging.Logger;

/**
 * Class that contain all GridEngineDaemon configuration settings
 * It manages a configuration file and/or static settings
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 * @see GridEngineDaemon
 */
public class GridEngineDaemonConfig {
    /*
      Logger
    */
    private static final Logger _log = Logger.getLogger(GridEngineDaemonLogger.class.getName());
    
    public static final String LS = System.getProperty("line.separator");

    /*
      Configuration file
    */
    private final String gedPropetiesFile = "GridEngineDaemon.properties";
    /*
       Database settings
     */    
    private String apisrv_dbhost = "localhost";
    private String apisrv_dbport = "3306";
    private String apisrv_dbuser = "geapiserver";
    private String apisrv_dbpass = "geapiserver_password";
    private String apisrv_dbname = "geapiserver";
    
    /*
      GrigEngineDaemon settings
    */
    private int gedMaxThreads   =  100;
    private int gedCloseTimeout =   20;
    
    /*
      GridEngineDaemonPolling settings
    */
    private int gePollingDelay       = 4000;
    private int gePollingMaxCommands =    5;
    
    /*
      GridEngineDaemonController settings
    */
    private int geControllerDelay       =10000;
    private int geControllerMaxCommands =    5;
    
    /*
     GridEngine UsersTracking DB
    */    
    private String utdb_host = "localhost";
    private String utdb_port = "3306";
    private String utdb_user = "tracking_user";
    private String utdb_pass = "usertracking";
    private String utdb_name = "userstracking";             
    
    /**
     * Load the given configuration file which overrides static settings
     * @param configFile 
     */
    public GridEngineDaemonConfig() { 
        /*
          Load a configuration file containing GridEngineDaemon settings
          wich override the static settings defined in the class
        */    
        loadProperties();
        _log.info("GridEngineDaemon config:"+LS+this.toString());
    }
    
    /**
     * Load GridEngineDaemon.properties values
     */
    private void loadProperties() {
        InputStream inputStream=null;        
        Properties prop = new Properties();
        try {
            inputStream = this.getClass().
                    getResourceAsStream(gedPropetiesFile);            
            
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
                    
            // GridEngineDaemon thread settings
            String prop_gedMaxThreads = prop.getProperty("gedMaxThreads");
            String prop_gedCloseTimeout = prop.getProperty("gedCloseTimeout");
                    
            // GridEngineDaemonPolling settings
            String prop_gePollingDelay = prop.getProperty("gePollingDelay");
            String prop_gePollingMaxCommands = prop.getProperty("gePollingMaxCommands");
            
            // GridEngineDaemonController settings
            String prop_geControllerDelay = prop.getProperty("geControllerDelay");
            String prop_geControllerMaxCommands = prop.getProperty("geControllerMaxCommands");
            
            // GridEngine' UsersTracking database settings
            String prop_utdb_host = prop.getProperty("utdb_host");
            String prop_utdb_port = prop.getProperty("utdb_host");            
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
            
            // GridEngineDaemon thread settings
            if(prop_gedMaxThreads!=null) 
                this.gedMaxThreads = Integer.parseInt(prop_gedMaxThreads);
            if(prop_gedCloseTimeout!=null) 
                this.gedCloseTimeout=Integer.parseInt(prop_gedCloseTimeout);
                    
            // GridEngineDaemonPolling settings
            if(prop_gePollingDelay!=null) 
                this.gePollingDelay=Integer.parseInt(prop_gePollingDelay);
            if(prop_gePollingMaxCommands!=null) 
                this.gePollingMaxCommands=Integer.parseInt(prop_gePollingMaxCommands);
            
            // GridEngineDaemonController settings
            if(prop_geControllerDelay!=null)
                this.geControllerDelay=Integer.parseInt(prop_geControllerDelay);
            if(prop_geControllerMaxCommands!=null)
                this.geControllerMaxCommands=Integer.parseInt(prop_geControllerMaxCommands);
            
            // GridEngine' UsersTracking database settings
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
            _log.warning("Unable to load property file; using default settings");
        } catch(IOException e) {
            _log.warning("Error reading file: "+ e);
        } catch(NumberFormatException e) {
            _log.warning("Error while reading property file: "+ e);
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
     * Get GridEngineDaemon database name
     * @return apisrv_dbname
     */
    public String getApisrv_dbname() {
        return apisrv_dbname;
    }
    /**
     * Set GridEngine database name
     * @param apisrv_dbname 
     */
    public void setApisrv_dbname(String apisrv_dbname) {
        this.apisrv_dbname = apisrv_dbname;
    }

    /**
     * Get GridEngineDaemon database host
     * @return apisrv_dbname
     */
    public String getApisrv_dbhost() {
        return apisrv_dbhost;
    }
    /**
     * Set GridEngine database host
     * @param apisrv_dbhost
     */
    public void setApisrv_dbhost(String apisrv_dbhost) {
        this.apisrv_dbhost = apisrv_dbhost;
    }

    /**
     * Get GridEngineDaemon database port
     * @return apisrv_dbport
     */
    public String getApisrv_dbport() {
        return apisrv_dbport;
    }
    /**
     * Set GridEngine database port
     * @param apisrv_dbport
     */
    public void setApisrv_dbport(String apisrv_dbport) {
        this.apisrv_dbport = apisrv_dbport;
    }

    /**
     * Get GridEngineDaemon database user
     * @return apisrv_dbuser
     */
    public String getApisrv_dbuser() {
        return apisrv_dbuser;
    }
    /**
     * Set GridEngine database user
     * @param apisrv_dbuser
     */
    public void setApisrv_dbuser(String apisrv_dbuser) {
        this.apisrv_dbuser = apisrv_dbuser;
    }

    /**
     * Get GridEngineDaemon database password
     * @return apisrv_pass
     */
    public String getApisrv_dbpass() {
        return apisrv_dbpass;
    }
    /**
     * Set GridEngine database password
     * @param apisrv_dbpass
     */
    public void setApisrv_dbpass(String apisrv_dbpass) {
        this.apisrv_dbpass = apisrv_dbpass;
    }

    /**
     * Get GridEngineDaemon thread closure timeout
     * @return gedCloseTimeout number of seconds waiting for thread closure
     */public int getGedCloseTimeout() {
        return gedCloseTimeout;
    }
    /**
     * Set GridEngineDaemon thread closure timeout
     * @param gedCloseTimeout 
     */
    public void setGedCloseTimeout(int gedCloseTimeout) {
        this.gedCloseTimeout = gedCloseTimeout;
    }
        
    /*
      GridEngineDaemon
    */
    
    /**
     * Get GridEngineDaemon max number of threads
     * @return gedMaxThreads maximum number of threads
     */
    public int getMaxThreads() { 
        return gedMaxThreads; 
    }
    /**
     * Set GridEngineDaemon max number of threads
     * @param gedMaxThreads maximum number of threads
     */
    public void setMaxThreads(int gedMaxThreads) { 
        this.gedMaxThreads=gedMaxThreads; 
    }
    
    /**
     * Get GridEngineDaemon closing thread timeout value
     * @return gedMaxThreads maximum number of threads
     */
    public int getCloseTimeout() { 
        return gedCloseTimeout; 
    }
    /**
     * Set GridEngineDaemon closing thread timeout value
     * @param gedCloseTimeout closing thread timeout value
     */
    public void setCloseTimeout(int gedCloseTimeout) { 
        this.gedCloseTimeout=gedCloseTimeout; 
    }
    
    /*
      GridEngineDaemonPolling settings
    */  
    
    /**
     * Get polling thread loop delay
     * @return gePollingDelay number of seconds for each controller loop
     */
    public int getPollingDelay() {
        return gePollingDelay;
    }
    /**
     * Set polling thread loop delay
     * @param gePollingDelay 
     */
    public void setPollingDelay(int gePollingDelay) {
        this.gePollingDelay = gePollingDelay;
    }
    /**
     * Get polling thread max number of commands per loop
     * @return gePollingMaxCommands number of records to be extracted 
     * from ge_queue table
     */
    public int getPollingMaxCommands() {
        return gePollingMaxCommands;
    }
    /**
     * Set polling thread max number of commands per loop
     * @param gePollingMaxCommands 
     */
    public void setPollingMaxCommands(int gePollingMaxCommands) {
        this.gePollingMaxCommands = gePollingMaxCommands;
    }

    /*
      GridEngineDaemonController settings
    */  
    
    /**
     * Get controller thread loop delay
     * @return geControllerDelay number of seconds for each controller loop
     */
    public int getControllerDelay() {
        return geControllerDelay;
    }
    /**
     * Set controller thread loop delay
     * @param geControllerDelay 
     */
    public void setControllerDelay(int geControllerDelay) {
        this.geControllerDelay = geControllerDelay;
    }
    /**
     * Get controller thread max number of commands per loop
     * @return geControllerMaxCommands number of records to be extracted 
     * from ge_queue table
     */
    public int getControllerMaxCommands() {
        return geControllerMaxCommands;
    }
    /**
     * Set controller thread max number of commands per loop
     * @param geControllerMaxCommands 
     */
    public void setControllerMaxCommands(int geControllerMaxCommands) {
        this.geControllerMaxCommands = geControllerMaxCommands;
    }
    
    /*
      GridEngine database settings
    */
    
    /**
     * Return the GridEngine' userstracking database host
     * @return userstracking database host
     */
    String getGridEngine_db_host() { return this.utdb_host; }
    /**
     * Set the GridEngine' userstracking database host
     * @param userstracking database host
     */
    void getGridEngine_db_host(String utdb_host) { this.utdb_host=utdb_host; } 
    /**
     * Return the GridEngine' userstracking database port
     * @return userstracking database port
     */
    String getGridEngine_db_port() { return this.utdb_port; }
    /**
     * Set the GridEngine' userstracking database port
     * @param userstracking database port
     */
    void getGridEngine_db_port(String utdb_port) { this.utdb_port=utdb_port; } 
    /**
     * Return the GridEngine' userstracking database user
     * @return userstracking database user
     */
    String getGridEngine_db_user() { return this.utdb_user; }
    /**
     * Set the GridEngine' userstracking database user
     * @param userstracking database user
     */
    void getGridEngine_db_user(String utdb_user) { this.utdb_user=utdb_user; } 
    /**
     * Return the GridEngine' userstracking database password
     * @return userstracking database password
     */
    String getGridEngine_db_pass() { return this.utdb_pass; }
    /**
     * Set the GridEngine' userstracking database password
     * @param userstracking database password
     */
    void getGridEngine_db_pass(String utdb_pass) { this.utdb_pass=utdb_pass; } 
    /**
     * Return the GridEngine' userstracking database name
     * @return userstracking database name
     */
    String getGridEngine_db_name() { return this.utdb_name; }
    /**
     * Set the GridEngine' userstracking database name
     * @param userstracking database name
     */
    void getGridEngine_db_name(String utdb_name) { this.utdb_name=utdb_name; } 
    
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
              +"[GridEningeDaemon settings]"                            +"'"+LS
              +"    gedMaxThreads   : '"        +gedMaxThreads          +"'"+LS
              +"    gedCloseTimeout : '"        +gedCloseTimeout        +"'"+LS
              +"[GridEningeDaemonPolling settings]"                     +"'"+LS
              +"    gePollingDelay       : '"   +gePollingDelay         +"'"+LS
              +"    gePollingMaxCommands : '"   +gePollingMaxCommands   +"'"+LS 
              +"[GridEningeDaemonController settings]"                  +"'"+LS
              +"    geControllerDelay       : '"+geControllerDelay      +"'"+LS
              +"    geControllerMaxCommands : '"+geControllerMaxCommands+"'"+LS 
              +"[GridEngine UsersTracking DB settings]"                     +LS
              +"    db_host : '"                +utdb_host              +"'"+LS
              +"    db_port : '"                +utdb_port              +"'"+LS
              +"    db_user : '"                +utdb_user              +"'"+LS        
              +"    db_pass : '"                +utdb_pass              +"'"+LS        
              +"    db_name : '"                +utdb_name              +"'"+LS;                   
    }
}
