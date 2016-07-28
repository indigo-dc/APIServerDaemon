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

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * APIServerDaemon interface for TOSCA
 * 
 * @author brunor
 */
public class ToscaIDCInterface {
    /*
     * Logger
     */
    private static final Logger _log             = Logger.getLogger(SimpleToscaInterface.class.getName());
    public static final String  LS               = System.getProperty("line.separator");
    public static final String  FS               = System.getProperty("file.separator");
    public static final String  JO               = "jobOutput";
    String                      APIServerConnURL = "";
    APIServerDaemonCommand      toscaCommand;
    APIServerDaemonConfig       asdConfig;
    
    /**
     *  Tosca parameters
     */
    URL toscaEndPoint = null;
    String toscaToken = "";

    /**
     * Empty constructor for SimpleToscaInterface
     */
    public ToscaIDCInterface() {
        _log.debug("Initializing SimpleToscaInterface");
        System.setProperty("saga.factory", "fr.in2p3.jsaga.impl.SagaFactoryImpl");
    }

    /**
     * Constructor for SimpleTosca taking as input a given command
     */
    public ToscaIDCInterface(APIServerDaemonCommand toscaCommand) {
        this();
        _log.debug("SimpleTosca command:" + LS + toscaCommand);
        this.toscaCommand = toscaCommand;
    }

    /**
     * Constructor for SimpleToscaInterface taking as input the
     * APIServerDaemonConfig and a given command
     */
    public ToscaIDCInterface(APIServerDaemonConfig asdConfig, APIServerDaemonCommand toscaCommand) {
        this(toscaCommand);
        setConfig(asdConfig);
    }
    
    /**
     * Load APIServerDaemon  configuration settings
     * @param Config APIServerDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig asdConfig) {
        this.asdConfig        = asdConfig;
        this.APIServerConnURL = asdConfig.getApisrv_URL();
    }
    
    /**
     * Submit to TOSCA a template
     */
     private String submitTosca(String tosca_template) {
         
        StringBuilder orchestrator_result=new StringBuilder("");
        StringBuilder postData = new StringBuilder();
        postData.append("{ \"template\": \"");
        String tosca_template_content="";
        String tosca_UUID="";
        try {            
            tosca_template_content = new String(Files.readAllBytes(Paths.get(tosca_template))).replace("\n", "\\n"); 
            postData.append(tosca_template_content);
        } catch (IOException ex) {
            _log.error("Template '"+tosca_template+"'is not readable");
        }
        postData.append("\" }");

        _log.debug("JSON Data prepared: \n" + postData);
        
        HttpURLConnection conn;
        String orchestratorDoc="";
        try {
            conn = (HttpURLConnection) toscaEndPoint.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty ("Authorization: Bearer",toscaToken);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("charset", "utf-8");
            conn.setDoOutput(true);
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(postData.toString());
            wr.flush();
            wr.close();
            _log.debug("Orchestrator status code: " + conn.getResponseCode());
            _log.debug("Orchestrator status message: " + conn.getResponseMessage());
            if (conn.getResponseCode() == 201) {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                orchestrator_result = new StringBuilder();
                String ln;
                while ((ln = br.readLine()) != null) {
                    orchestrator_result.append(ln);
                }                
                _log.debug("Orchestrator result: " + orchestrator_result);
                orchestratorDoc = orchestrator_result.toString();
                tosca_UUID = getDocumentValue(orchestratorDoc,"uuid");
                _log.debug("Created resource has UUID: '"+tosca_UUID+"'");                              
            }
        } catch (IOException ex) {
            _log.error("Connection error with the service at " + toscaEndPoint.toString());
            _log.error(ex);            
        } catch (ParseException ex) {
            _log.error("Error parsing JSON:" + orchestratorDoc);
            _log.error(ex);            
        }
        return tosca_UUID;
    }
     
    /**
     * Read values from the json.
     *
     * @param json The json from where to
     * @param key The element to return. It can retrieve nested elements
     * providing the full chain as
     * &lt;element&gt;.&lt;element&gt;.&lt;element&gt;
     * @return The element value
     * @throws ParseException If the json cannot be parsed
     */
    protected String getDocumentValue(String json, String key) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(json);
        String keyelement[] = key.split("\\.");
        for (int i = 0; i < (keyelement.length - 1); i++) {
            jsonObject = (JSONObject) jsonObject.get(keyelement[i]);
        }
        return (String) jsonObject.get(keyelement[keyelement.length - 1]);
    }
}
