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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
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
    private static final Logger _log = Logger.getLogger(ToscaIDCInterface.class.getName());
    public static final String LS = System.getProperty("line.separator");
    public static final String FS = System.getProperty("file.separator");
    public static final String JO = "jobOutput";
    String APIServerConnURL = "";
    APIServerDaemonCommand toscaCommand;
    APIServerDaemonConfig asdConfig;

    /**
     * Tosca parameters
     */
    URL toscaEndPoint = null;
    String toscaToken = "";
    String toscaTemplate = "";
    String toscaParameters = "";
    String informtativeFile = "";
    String toscaUUID = "";
    ToscaIDCInterfaceDB tii = null;

    /**
     * Empty constructor for ToscaIDCInterface
     */
    public ToscaIDCInterface() {
	_log.debug("Initializing ToscaIDCInterface");
	System.setProperty("saga.factory", "fr.in2p3.jsaga.impl.SagaFactoryImpl");
    }

    /**
     * Constructor for ToscaIDCInterface taking as input a given command
     */
    public ToscaIDCInterface(APIServerDaemonCommand toscaCommand) {
	this();
	_log.debug("ToscaIDC command:" + LS + toscaCommand);
	this.toscaCommand = toscaCommand;
    }

    /**
     * Constructor for ToscaIDCInterface taking as input the
     * APIServerDaemonConfig and a given command
     */
    public ToscaIDCInterface(APIServerDaemonConfig asdConfig, APIServerDaemonCommand toscaCommand) {
	this(toscaCommand);
	setConfig(asdConfig);
	tii = new ToscaIDCInterfaceDB(APIServerConnURL);
	if (tii == null)
	    _log.error("Unable to instantiate ToscaIDCInterface DB class");
	else
	    _log.debug("Syccessfully instantiate ToscaIDCInterface DB class");
    }

    /**
     * Load APIServerDaemon configuration settings
     * 
     * @param Config
     *            APIServerDaemon configuration object
     */
    public void setConfig(APIServerDaemonConfig asdConfig) {
	this.asdConfig = asdConfig;
	this.APIServerConnURL = asdConfig.getApisrvURL();
    }

    /**
     * Return the ToscaIDCInterface resource information file path
     */
    public String getInfoFilePath() {
	return toscaCommand.getActionInfo() + FS + toscaCommand.getTaskId() + "_toscaIDC.json";
    }

    /**
     * process JSON object containing information stored in file: <action_info>/
     * <task_id>.json and submit using tosca adaptor
     */
    public int submitTosca() {
	int simple_tosca_id = 0;
	org.json.JSONObject jsonJobDesc = null;

	_log.debug("Entering sumitTosca (ToscaIDC)");

	String jobDescFileName = toscaCommand.getActionInfo() + FS + toscaCommand.getTaskId() + ".json";
	_log.debug("JSON filename: '" + jobDescFileName + "'");
	try {
	    // Prepare jobOutput dir for output sandbox
	    String outputSandbox = toscaCommand.getActionInfo() + FS + JO;
	    _log.debug("Creating job output directory: '" + outputSandbox + "'");
	    File outputSandboxDir = new File(outputSandbox);
	    if (!outputSandboxDir.exists()) {
		_log.debug("Creating job output directory");
		outputSandboxDir.mkdir();
		_log.debug("Job output successfully created");
	    } else {
		// Directory altready exists; clean all its content
		_log.debug("Cleaning job output directory");
		FileUtils.cleanDirectory(outputSandboxDir);
		_log.debug("Successfully cleaned job output directory");
	    }

	    // Now read values from JSON and prepare the submission accordingly
	    InputStream is = new FileInputStream(jobDescFileName);
	    String jsonTxt = IOUtils.toString(is);

	    jsonJobDesc = (org.json.JSONObject) new org.json.JSONObject(jsonTxt);
	    _log.debug("Loaded APIServer JobDesc:\n" + LS + jsonJobDesc);

	    // Username (unused yet but later used for accounting)
	    String user = String.format("%s", jsonJobDesc.getString("user"));
	    _log.debug("User: '" + user + "'");

	    // Get app Info and Parameters
	    org.json.JSONObject appInfo = new org.json.JSONObject();
	    appInfo = jsonJobDesc.getJSONObject("application");
	    JSONArray appParams = new JSONArray();
	    appParams = appInfo.getJSONArray("parameters");

	    // Application parameters
	    String executable = "";
	    String output = "";
	    String error = "";
	    String arguments = "";

	    for (int i = 0; i < appParams.length(); i++) {
		org.json.JSONObject appParameter = appParams.getJSONObject(i);

		// Get parameter name and value
		String param_name = appParameter.getString("param_name");
		String param_value = appParameter.getString("param_value");

		switch (param_name) {
		case "target_executor":
		    _log.debug("target_executor: '" + param_value + "'");
		    break;

		case "jobdesc_executable":
		    executable = param_value;
		    _log.debug("executable: '" + executable + "'");
		    break;

		case "jobdesc_output":
		    output = param_value;
		    _log.debug("output: '" + output + "'");
		    break;

		case "jobdesc_error":
		    error = param_value;
		    _log.debug("error: '" + error + "'");
		    break;

		case "jobdesc_arguments":
		    arguments = param_value;
		    _log.debug("arguments: '" + arguments + "'");
		    break;

		default:
		    _log.warn("Unsupported application parameter name: '" + param_name + "' with value: '" + param_value
			    + "'");
		}
	    }

	    // Arguments
	    String job_args = arguments;
	    JSONArray jobArguments = jsonJobDesc.getJSONArray("arguments");

	    for (int j = 0; j < jobArguments.length(); j++) {
		job_args += ((job_args.length() > 0) ? "," : "") + jobArguments.getString(j);
	    }

	    String[] args = job_args.split(",");

	    for (int k = 0; k < args.length; k++) {
		_log.debug("args[" + k + "]: '" + args[k] + "'");
	    }

	    // Infrastructures
	    // Select one of the possible infrastructures among the enabled ones
	    // A random strategy is currently implemented; this could be changed
	    // later
	    JSONArray jobInfrastructures = appInfo.getJSONArray("infrastructures");
	    JSONArray enabledInfras = new JSONArray();

	    for (int v = 0, w = 0; w < jobInfrastructures.length(); w++) {
		org.json.JSONObject infra = jobInfrastructures.getJSONObject(w);

		if (infra.getString("status").equals("enabled")) {
		    enabledInfras.put(v++, infra);
		}
	    }

	    int selInfraIdx = 0;
	    Random rndGen = new Random();

	    if (enabledInfras.length() > 1) {
		selInfraIdx = rndGen.nextInt(enabledInfras.length());
	    }

	    org.json.JSONObject selInfra = new org.json.JSONObject();

	    selInfra = enabledInfras.getJSONObject(selInfraIdx);
	    _log.debug("Selected infra: '" + LS + selInfra.toString(4) + "'");

	    // Infrastructure parameters
	    JSONArray infraParams = selInfra.getJSONArray("parameters");

	    for (int h = 0; h < infraParams.length(); h++) {
		org.json.JSONObject infraParameter = infraParams.getJSONObject(h);
		String param_name = infraParameter.getString("name");
		String param_value = infraParameter.getString("value");

		switch (param_name) {
		case "tosca_endpoint":
		    toscaEndPoint = new URL(param_value);
		    toscaCommand.setRunTimeData("tosca_endpoint", toscaEndPoint.toString(), "TOSCA endpoint", "", "");
		    _log.debug("tosca_endpoint: '" + toscaEndPoint + "'");

		    break;

		case "tosca_template":
		    toscaTemplate = toscaCommand.getActionInfo() + "/" + param_value;
		    _log.debug("tosca_template: '" + toscaTemplate + "'");

		    break;

		case "tosca_parameters":
		    toscaParameters += ((toscaParameters.length() > 0) ? "&" : "") + param_value;
		    _log.debug("tosca_parameters: '" + toscaParameters + "'");

		    break;

		default:
		    _log.warn("Ignoring infrastructure parameter name: '" + param_name + "' with value: '" + param_value
			    + "'");
		}
	    }

	    // Prepare JSAGA IO file list
	    String IOFiles = "";
	    JSONArray inputFiles = jsonJobDesc.getJSONArray("input_files");

	    for (int i = 0; i < inputFiles.length(); i++) {
		org.json.JSONObject fileEntry = inputFiles.getJSONObject(i);
		String fileName = fileEntry.getString("name");

		IOFiles += ((IOFiles.length() > 0) ? "," : "") + toscaCommand.getActionInfo() + FS
			+ fileEntry.getString("name") + ">" + fileEntry.getString("name");
	    }

	    JSONArray outputFiles = jsonJobDesc.getJSONArray("output_files");

	    for (int j = 0; j < outputFiles.length(); j++) {
		org.json.JSONObject fileEntry = outputFiles.getJSONObject(j);
		String fileName = fileEntry.getString("name");

		IOFiles += ((IOFiles.length() > 0) ? "," : "") + toscaCommand.getActionInfo() + FS + JO + FS
			+ fileEntry.getString("name") + "<" + fileEntry.getString("name");
	    }

	    _log.debug("IOFiles: '" + IOFiles + "'");

	    String files[] = IOFiles.split(",");

	    for (int i = 0; i < files.length; i++) {
		_log.debug("IO Files[" + i + "]: '" + files[i] + "'");
	    }

	    // Add info file name to toscaParameters
	    toscaParameters += ((toscaParameters.length() > 0) ? "&" : "") + "info=" + toscaCommand.getActionInfo() + FS
		    + toscaCommand.getTaskId() + "_toscaIDC.json";
	    _log.debug("TOSCA parameters: '" + toscaParameters + "'");

	    toscaToken = tii.getToken(toscaCommand);
	    _log.debug("Token for toscaCommand having id: '" + toscaCommand.getTaskId() + "' is: '" + toscaToken + "'");

	    // Finally submit the job
	    toscaUUID = submitOrchestrator();
	    _log.info("toscaUUID: '" + toscaUUID + "'");

	    // Register JobId, if targetId exists it is a submission retry
	    try {
		String submitStatus = "SUBMITTED";
		int toscaTargetId = toscaCommand.getTargetId();

		if (toscaTargetId > 0) {

		    // update tosca_id if successful
		    if ((toscaUUID != null) && (toscaUUID.length() > 0)) {
			tii.updateToscaId(toscaTargetId, toscaUUID);
		    } else {
			submitStatus = "ABORTED";
		    }

		    toscaCommand.setTargetStatus(submitStatus);
		    tii.updateToscaStatus(toscaTargetId, submitStatus);
		    _log.debug("Updated existing entry in simple_tosca (ToscaIDC) table at id: '" + toscaTargetId + "'"
			    + "' - status: '" + submitStatus + "'");
		} else {
		    _log.debug("Creating a new entry in simple_tosca table for submission: '" + toscaUUID + "'");

		    if (toscaUUID.length() == 0) {
			submitStatus = "ABORTED";
		    }

		    toscaCommand.setTargetStatus(submitStatus);
		    simple_tosca_id = tii.registerToscaId(toscaCommand, toscaUUID, toscaEndPoint.toString(),
			    submitStatus);

		    _log.debug("Registered in simple_tosca (ToscaIDC) with id: '" + simple_tosca_id + "' - status: '"
			    + submitStatus + "'");
		}
	    } catch (Exception e) {
		_log.fatal("Unable to register tosca_id: '" + toscaUUID + "'");
	    } finally {
		toscaCommand.update();
	    }
	} catch (SecurityException se) {
	    _log.error("Unable to create job output folder in: '" + toscaCommand.getActionInfo() + "' directory");
	} catch (Exception ex) {
	    _log.error("Caught exception: '" + ex.toString() + "'");
	}

	return simple_tosca_id;
    }

    /**
     * Submit template to the orchestrator a template
     */
    public String submitOrchestrator() {

	StringBuilder orchestrator_result = new StringBuilder("");
	StringBuilder postData = new StringBuilder();
	String tosca_parameters = "";
	String tosca_parameters_json = "";
	String tosca_UUID = "";
	String toscaParams[] = toscaParameters.split("&");
	String toscaParameters = "";
	for (int i = 0; i < toscaParams.length; i++) {
	    String param_args[] = toscaParams[i].split("=");
	    if (param_args[0].trim().equals("params")) {
		tosca_parameters_json = toscaCommand.getActionInfo() + FS + param_args[1].trim();
		_log.debug("Loading params json file: '" + tosca_parameters_json + "'");
		try {
		    String params_json = new String(Files.readAllBytes(Paths.get(tosca_parameters_json)));
		    _log.debug("params JSON: '" + params_json + "'");
		    tosca_parameters = getDocumentValue(params_json, "parameters");
		    _log.debug("extracted parameters: '" + toscaParameters + "'");
		} catch (IOException ex) {
		    _log.error("Parameters json file '" + tosca_parameters_json + "' is not readable");
		    _log.error(ex);
		} catch (ParseException ex) {
		    _log.error("Parameters json file '" + tosca_parameters_json + "' is not parseable");
		    _log.error(ex);
		}
		_log.debug("Parameters json file '" + tosca_parameters_json + "' successfully parsed");
		break;
	    }
	}
	if (tosca_parameters.length() > 0) {
	    toscaParameters = "\"parameters\": " + tosca_parameters + ", ";
	}
	postData.append("{ " + toscaParameters + "\"template\": \"");
	String tosca_template_content = "";
	_log.debug("Escaping toscaTemplate file '" + toscaTemplate + "'");
	try {
	    tosca_template_content = new String(Files.readAllBytes(Paths.get(toscaTemplate))).replace("\n", "\\n");
	} catch (IOException ex) {
	    _log.error("Template '" + toscaTemplate + "'is not readable");
	    _log.error(ex);
	}
	postData.append(tosca_template_content);
	postData.append("\" }");
	_log.debug("JSON Data (begin):\n" + postData + "\nJSON Data (end)");

	HttpURLConnection conn;
	String orchestratorDoc = "";
	try {
	    conn = (HttpURLConnection) toscaEndPoint.openConnection();
	    conn.setRequestMethod("POST");
	    conn.setRequestProperty("Authorization", "Bearer " + toscaToken);
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
		tosca_UUID = getDocumentValue(orchestratorDoc, "uuid");
		_log.debug("Created resource has UUID: '" + tosca_UUID + "'");
	    } else {
		_log.warn("Orchestrator return code is: " + conn.getResponseCode());
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
     * @param json
     *            The json from where to
     * @param key
     *            The element to return. It can retrieve nested elements
     *            providing the full chain as
     *            &lt;element&gt;.&lt;element&gt;.&lt;element&gt;
     * @return The element value
     * @throws ParseException
     *             If the json cannot be parsed
     */
    protected String getDocumentValue(String json, String key) throws ParseException {
	JSONParser parser = new JSONParser();
	JSONObject jsonObject = (JSONObject) parser.parse(json);
	String keyelement[] = key.split("\\.");
	for (int i = 0; i < (keyelement.length - 1); i++) {
	    jsonObject = (JSONObject) jsonObject.get(keyelement[i]);
	}
	return jsonObject.get(keyelement[keyelement.length - 1]).toString();
    }

    // This method returns the job output dir used for this interface
    public static String getOutputDir() {
	return JO;
    }

    /**
     * Return deployment information of a given toscaUUID
     * 
     * @param toscaUUID
     * @return
     */
    protected String getToscaDeployment(String toscaUUID, String toscaToken) {
	StringBuilder deployment = new StringBuilder();
	HttpURLConnection conn;
	URL deploymentEndpoint = null;
	try {
	    _log.debug("endpoint: '" + toscaEndPoint + "'");
	    deploymentEndpoint = new URL(toscaEndPoint.toString() + "/" + toscaUUID);
	    _log.debug("deploymentEndpoint: '" + deploymentEndpoint + "'");
	    conn = (HttpURLConnection) deploymentEndpoint.openConnection();
	    conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Bearer " + toscaToken);
	    conn.setRequestProperty("Content-Type", "application/json");
	    conn.setRequestProperty("charset", "utf-8");
	    _log.debug("Orchestrator status code: " + conn.getResponseCode());
	    _log.debug("Orchestrator status message: " + conn.getResponseMessage());
	    if (conn.getResponseCode() == 200) {
		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String ln;
		while ((ln = br.readLine()) != null) {
		    deployment.append(ln);
		}
		_log.debug("Orchestrator result: " + deployment);
	    }
	} catch (IOException ex) {
	    _log.error("Connection error with the service at " + deploymentEndpoint.toString());
	    _log.error(ex);
	}
	return deployment.toString();
    }

    /**
     * GetStatus of TOSCA submission
     * 
     * @return
     */
    public String getStatus() {
	_log.debug("Entering IDC getStatus ...");
	String status = toscaCommand.getTargetStatus();
	try {
	    // toscaEndPoint = new
	    // URL(toscaCommand.getRunTimeData("tosca_endpoint"));
	    toscaEndPoint = new URL(tii.toscaEndPoint(toscaCommand));
	} catch (MalformedURLException ex) {
	    _log.error("Unable to get endpoint from command: '" + toscaCommand + "'");
	}
	_log.debug("tosca endpoint: '" + toscaEndPoint + "'");
	String toscaUUID = tii.getToscaId(toscaCommand);
	_log.debug("tosca UUID: '" + toscaUUID + "'");
        String toscaToken = tii.getToken(toscaCommand);
	_log.debug("tosca Token: '" + toscaToken + "'");
	String toscaDeploymentInfo = getToscaDeployment(toscaUUID,toscaToken);
	_log.debug("tosca deployment info: '" + toscaDeploymentInfo + "'");
	try {
	    status = getDocumentValue(toscaDeploymentInfo, "status");
	} catch (ParseException ex) {
	    _log.error("Unable to parse deployment result: '" + toscaDeploymentInfo + "'");
	}
	// Do status mapping (orchestrator->JSAGA style)
	// Check for DONE status; this saves the informative file
	if (status.equals("CREATE_COMPLETE")) {
	    status = "DONE";
	    // When deployment is done save in runtime data outputs field
	    informtativeFile = getInfoFilePath();
	    try {
		String outputs = getDocumentValue(toscaDeploymentInfo, "outputs");
		_log.debug("Output for deployment having UUID: '" + toscaUUID + "' is: '" + outputs + "'");
		saveInformativeFile(outputs);
		toscaCommand.setRunTimeData("tosca_outputs", informtativeFile, "TOSCA deployiment outputs field",
			"file://", "plain/text");
		_log.debug("Successfully generated informativeFile at: '" + informtativeFile
			+ "' and registered it on runtime data");
	    } catch (Exception ex) {
		_log.error("Unable to parse deployment info: '" + toscaDeploymentInfo + "' looking for outputs field");
	    }
	} else if (status.equals("CREATE_FAILED"))
	    status = "ABORT";
	else if (status.equals("CREATE_IN_PROGRESS"))
	    status = "RUNNING";
	else
	    // status = "UNKNOWN";
	    _log.error("Unhespected ToscaIDC status: '" + status + "'");
	_log.debug("Status of deployment having id: '" + toscaUUID + "' is: '" + status + "'");
	// update target status
	tii.updateToscaStatus(toscaCommand.getTargetId(), status);
	_log.debug("Leaving IDC getStatus");
	return status;
    }

    /**
     * Delete deployment having the given UUID
     * 
     * @param toscaUUID
     */
    protected void deleteToscaDeployment(String toscaUUID) {
	StringBuilder deployment = new StringBuilder();
	HttpURLConnection conn;
	try {
	    URL deploymentEndpoint = new URL(toscaEndPoint.toString() + "/" + toscaUUID);
	    conn = (HttpURLConnection) deploymentEndpoint.openConnection();
	    conn.setRequestMethod("DELETE");
            conn.setRequestProperty("Authorization", "Bearer " + toscaToken);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("charset", "utf-8");
	    _log.debug("Orchestrator status code: " + conn.getResponseCode());
	    _log.debug("Orchestrator status message: " + conn.getResponseMessage());
	    if (conn.getResponseCode() == 204) {
		_log.debug("Successfully removed resource: '" + toscaUUID + "'");
	    } else {
		_log.error("Unable to remove resource: '" + toscaUUID + "'");
	    }
	} catch (IOException ex) {
	    _log.error("Connection error with the service at " + toscaEndPoint.toString());
	    _log.error(ex);
	}
    }

    /**
     * Save informative file
     * 
     * @param infoData
     */
    protected void saveInformativeFile(String infoData) {
	Writer writer = null;

	try {
	    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(informtativeFile), "utf-8"));
	    writer.write(infoData);
	    _log.debug("Saved info file: '" + informtativeFile + "' data: '" + infoData + "'");
	} catch (IOException ex) {
	    _log.error("Unable to save info file: '" + informtativeFile + "' data: '" + infoData + "'");
	} finally {
	    try {
		writer.close();
	    } catch (Exception ex) {
		/* ignore */}
	}
    }

}
