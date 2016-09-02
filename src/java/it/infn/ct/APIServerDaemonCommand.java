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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 * Class containing APIServerDaemon commands as registered in APIServer table
 * asd_queue.
 *
 * @author <a href="mailto:riccardo.bruno@ct.infn.it">Riccardo Bruno</a>(INFN)
 */
public class APIServerDaemonCommand {

    /**
     * Logger object.
     */
    private static final Logger LOG =
            Logger.getLogger(APIServerDaemonCommand.class.getName());

    /**
     * Line separator constant.
     */
    private static final String LS = System.getProperty("line.separator");

    /*
     * APIServer command fields
     */

    /**
     * Queue command task table identifier.
     */
    private int cmdTaskId;
    /**
     * Queue command action name: 'SUBMIT, CLEAN, ...'.
     */
    private String cmdAction;
    /**
     * The name of the involved target executor.
     */
    private String cmdTarget;
    /**
     * The target execution table record identifier.
     */
    private int cmdTargetId;
    /**
     * The status of the queue command: 'QUEUED, PROCESSING, PROCESSED, ...'.
     */
    private String cmdStatus;
    /**
     * The status of the queue command in the targeted infrastructure.
     */
    private String cmdTargetStatus;
    /**
     * Queue command retry count.
     */
    private int cmdRetry;
    /**
     * Queue command creation timestamp.
     */
    private Date cmdCreation;
    /**
     * Queue command lasst change timestamp.
     */
    private Date cmdLastChange;
    /**
     * Queue command last check timestamp.
     * @see APIServerDaemonCheckCommand
     */
    private Date cmdCheck;
    /**
     * Queue command directory path with task information.
     */
    private String cmdInfo;
    /**
     * Command record modify flag.
     */
    private boolean modifyFlag;
    /**
     * APIServer database connection URL.
     */
    private String asdConnectionURL;

    /**
     * Empty constructor, initialize with dummy/null values.
     */
    public APIServerDaemonCommand() {
    cmdTaskId = -1;
    cmdTargetId = -1;
    cmdAction = null;
    cmdTarget = null;
    cmdStatus = null;
    cmdTargetStatus = null;
    cmdRetry = -1;
    cmdCreation = null;
    cmdLastChange = null;
    cmdCheck = null;
    cmdInfo = null;
    modifyFlag = false;
    }

    /**
     * Constructor taking all GridEngineDaemon command values.
     * @param taskId - Task identifier in tasks table
     * @param targetId - Id of the cmdTarget executor record index
     * @param targetName - Name of the targeted executor interface
     * @param commandAction - Action of the command: SUBMIT, CLEAN, ...
     * @param asdConnURL - APIServer database connection URL
     * @param commandStatus - Status of the command: QUEUED, PROCESSING, ...
     * @param targetStatus - Status hold by cmdTarget executor interface
     * @param retryCount - Number of command execution attempts
     * @param commandCreation - Creation timestamp of queue command record
     * @param commandChange - Last change timestamp of queue command record
     * @param commandCheck - Last check timestamp of the queue command record
     * @param commandInfo - Firectory path name collecting task information
     *
     */
    public APIServerDaemonCommand(final String asdConnURL,
                                  int taskId,
                                  int targetId,
                                  String targetName,
                                  String commandAction,
                                  String commandStatus,
                                  String targetStatus,
                                  int retryCount,
                                  Date commandCreation,
                                  Date commandChange,
                                  Date commandCheck,
                                  String commandInfo) {
    this();
    cmdTaskId = taskId;
    cmdTargetId = targetId;
    cmdTarget = targetName;
    cmdAction = commandAction;
    cmdStatus = commandStatus;
    cmdTargetStatus = targetStatus;
    cmdRetry = retryCount;
    cmdCreation = commandCreation;
    cmdLastChange = commandChange;
    cmdCheck = commandCheck;
    cmdInfo = commandInfo;
    modifyFlag = false;
    asdConnectionURL = asdConnURL;
    }

    /**
     * Update the command values on the given DB
     */
    public void Update() {
        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS + toString());

            return;
        }

        if (isModified()) {
            try {
            LOG.debug("Opening connection for update command");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.updateCommand(this);
            validate();
            } catch (Exception e) {
            LOG.fatal("Unable to update command:" + LS
                    + this.toString() + LS + e.toString());
            } finally {
            // if (asdDB != null) {
            // asdDB.close();
            // }
            // LOG.debug("Closing connection for update command");
            }
        }
    }

    /**
     * Update the command check timestamp, this call is independent from the
     * validation flag
     */
    public void checkUpdate() {
        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS + toString());

            return;
        }

        try {
            LOG.debug("Opening connection for checkupdate command");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.checkUpdateCommand(this);
        } catch (Exception e) {
            LOG.fatal("Unable to update check timestamp for command:" + LS
                    + this.toString()
                    + LS + e.toString());
        } finally {
            // if (asdDB != null) {
            // asdDB.close();
            // }
            // LOG.debug("Closing connection for chekupdate command");
        }
    }

    /**
     * Invalidate modified flag
     */
    public void invalidate() {
        this.modifyFlag = true;
    }

    /**
     * Retry the command setting values: cmdStatus = QUEUED cmdCreation = now()
 cmdLastChange = now() increase current cmdRetry
     */
    void retry() {
        setStatus("QUEUED");

        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS + toString());

            return;
        }

        try {
            LOG.debug("Opening connection for retry command");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.retryTask(this);
        } catch (Exception e) {
            LOG.fatal("Unable retry task related to given command:" + LS
                    + this.toString());
        } 
    }

    /**
     * Serialize in a JSON string format the APIServerDaemon command values.
     */
    @Override
    public String toString() {
    DateFormat dFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    return "{" + LS
         + "[DB Values]" + LS
         + "  \"task_id\"      : \"" + cmdTaskId + "\"" + LS
         + "  \"target\"       : \"" + cmdTarget + "\"" + LS
         + "  \"target_id\"    : \"" + cmdTargetId + "\"" + LS
         + "  \"action\"       : \"" + cmdAction + "\"" + LS
         + "  \"status\"       : \"" + cmdStatus + "\"" + LS
         + "  \"target_status\": \"" + cmdTargetStatus + "\"" +LS
         + "  \"retry\"        : \"" + cmdRetry + "\"" + LS
         + "  \"creation\"     : \"" + dFmt.format(cmdCreation) + "\"" + LS
         + "  \"last_change\"  : \""
         + dFmt.format(cmdLastChange) + "\"" + LS
         + "  \"check_ts\"     : \"" + dFmt.format(cmdCheck) + "\"" + LS
         + "  \"action_info\"  : \"" + cmdInfo + "\"" + LS
         + "[Obj Values]" + LS + "  \"modified_flag\": \""
         + modifyFlag + "\"" + LS
         + "}";
    }

    /**
     * Trash the command setting values: cmdStatus = FAILED so that the 
 polling loops will never take it.
     */
    void trash() {
        setStatus("FAILED");

        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS
                    + toString());

            return;
        }

        try {
            LOG.debug("Opening connection for trash command");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.trashTask(this);
        } catch (Exception e) {
            LOG.fatal("Unable trash task related to given command:" + LS
                    + this.toString());
        }
    }

    /**
     * Reset modified flag.
     */
    public void validate() {
        modifyFlag = false;
    }

    /**
     * Get the APIServerDaemon connectionURL string.
     * 
     * @return asdConnectionURL
     */
    public String getASDConnectionURL() {
        return asdConnectionURL;
    }

    /**
     * Set the APIServerDaemon connectionURL string.
     * 
     * @return asdConnectionURL
     */
    public void setASDConnectionURL(String asdConnURL) {
        asdConnectionURL = asdConnURL;
    }

    /**
     * Get APIServerCommand 'cmdAction' field value.
     * 
     * @return cmdAction
     */
    public String getAction() {
        return cmdAction;
    }

    /**
     * Set APIServerCommand 'cmdAction' field value.
     * 
     * @param commandAction
     */
    public void setAction(String commandAction) {
        if (cmdAction == null) {
            return;
        }

        if ((cmdAction == null) || !this.cmdAction.equals(commandAction)) {
            modifyFlag = true;
            cmdAction = commandAction;
        }
    }

    /**
     * Get APIServerCommand 'cmdInfo' field value.
     * 
     * @return cmdInfo
     */
    public String getActionInfo() {
        return cmdInfo;
    }

    /**
     * Set APIServerCommand 'cmdInfo' field value.
     * 
     * @param commandInfo
     */
    public void setActionInfo(String commandInfo) {
        if (commandInfo == null) {
            return;
        }

        if ((cmdInfo == null)
          || !cmdInfo.equals(commandInfo)) {
            modifyFlag = true;
            cmdInfo = commandInfo;
        }
    }

    /**
     * Get APIServerCommand 'cmdCheck' field value.
     * 
     * @return cmdCheck
     */
    public Date getCheckTS() {
        return cmdCheck;
    }

    /**
     * Get APIServerCommand 'cmdCreation' field value.
     * 
     * @return cmdCreation
     */
    public Date getCreation() {
        return cmdCreation;
    }

    /**
     * Set APIServerCommand 'cmdCreation' field value.
     * 
     * @param commandCreation
     */
    public void setCreation(Date commandCreation) {
        if (commandCreation == null) {
            return;
        }

        if ((cmdCreation == null)
          || !cmdCreation.equals(commandCreation)) {
            modifyFlag = true;
            cmdCreation = commandCreation;
        }
    }

    /**
     * Get APIServerCommand 'cmdLastChange' field value.
     * 
     * @return cmdLastChange
     */
    public Date getLastChange() {
        return cmdLastChange;
    }

    /**
     * Set APIServerCommand 'cmdLastChange' field value.
     * 
     * @param commandChange
     */
    public void setLastChange(Date commandChange) {
        if (commandChange == null) {
            return;
        }

        if ((cmdLastChange == null)
          || !cmdLastChange.equals(commandChange)) {
            modifyFlag = true;
            cmdLastChange = commandChange;
        }
    }

    /**
     * Return time difference in milliseconds between current and cmdCreation
 timestamps
     *
     * @return Number of milliseconds between current and cmdCreation timestamps
     */
    public long getLifetime() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date currentTimeStamp = new Date();

        LOG.debug("Current TS: " + df.format(currentTimeStamp) +
                " - creation: " + df.format(cmdCreation) + " [Millis: "
            + (currentTimeStamp.getTime() - cmdCreation.getTime()) + "]");

        return currentTimeStamp.getTime() - cmdCreation.getTime();
    }

    /**
     * Return true if any field has been modified by any of 'set' methods.
     */
    public boolean isModified() {
        return modifyFlag;
    }

    /**
     * Get APIServerCommand 'cmdRetry' field value.
     * 
     * @return cmdTargetStatus
     */
    public int getRetry() {
        return cmdRetry;
    }

    /**
     * Set APIServerCommand 'cmdRetry' field value.
     * 
     * @param retryCount
     */
    public void setRetry(int retryCount) {
        if (cmdRetry != retryCount) {
            modifyFlag = true;
            cmdRetry = retryCount;
        }
    }

    /**
     * Save a keyName,keyValue,keyDesc triple in runtime_data table This
     * function is used by adaptors to store resource related information
     * relative to the current task.
     * 
     * @param rtdKey - Key name
     * @param rtdValue - Key value
     * @param rtdDesc - Key description
     * @param rtdProto - Key data retrieval prototype
     * @param rtdType - Key data retrieval type
     */
    void setRunTimeData(String rtdKey,
                        String rtdValue,
                        String rtdDesc,
                        String rtdProto,
                        String rtdType) {
        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS
                    + toString());

            return;
        }

        try {
            LOG.debug("Opening connection for set command' runtime data");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            asdDB.setRunTimeData(rtdKey,
                                 rtdValue,
                                 rtdDesc,
                                 rtdProto,
                                 rtdType,
                                 this);
            LOG.debug("Set run time data (key=" + rtdKey
                    + ", value=" + rtdValue + ") to the given command:" + LS
                + toString());
        } catch (Exception e) {
            LOG.fatal("Unable set run time data (key=" + rtdKey
                    + ", value=" + rtdValue + ") to the given command:" + LS
                    + toString());
        } finally {
            // if (asdDB != null) {
            // asdDB.close();
            // }
            // LOG.debug("Closing connection for set command' runtime data");
        }
    }

    /**
     * Retrieve keyValue from a given keyName in runtime_data table This
     * function is used by adaptors to store resource related information
     * relative to the current task.
     * 
     * @param rtdKey - Key name
     */
    public String getRunTimeData(String rtdKey) {
        String rtdValue = "";
        APIServerDaemonDB asdDB = null;

        if ((asdConnectionURL == null) || (asdConnectionURL.length() == 0)) {
            LOG.error("Command with no connection URL defined" + LS
                    + toString());

            return rtdValue;
        }

        try {
            LOG.debug("Opening connection for get command' runtime data");
            asdDB = new APIServerDaemonDB(asdConnectionURL);
            rtdValue = asdDB.getRunTimeData(rtdKey, this);
            LOG.debug("Run time data for key = '" + rtdKey
                    + "' has value = '" + rtdValue
                    + "') from the given command:" + LS
                    + toString());
        } catch (Exception e) {
            LOG.fatal("Unable get run time data (key = '" + rtdKey
                    + "', value = '" + rtdValue
                    + "') from the given command:" + LS
                    + toString());
        }
        return rtdValue;
    }

    /**
     * Get APIServerCommand 'cmdStatus' field value.
     * 
     * @return cmdStatus
     */
    public String getStatus() {
        return cmdStatus;
    }

    /**
     * Set APIServerCommand 'cmdStatus' field value.
     * 
     * @param commandStatus
     */
    public void setStatus(String commandStatus) {
        if (commandStatus == null) {
            return;
        }

        if ((cmdStatus == null) || !cmdStatus.equals(commandStatus)) {
            modifyFlag = true;
            cmdStatus = commandStatus;
        }
    }

    /**
     * Get APIServerCommand 'cmdTarget' field value.
     * 
     * @return cmdTarget
     */
    public String getTarget() {
        return cmdTarget;
    }

    /**
     * Get APIServerCommand 'cmdTargetId' field value.
     * 
     * @return cmdTargetId (For instance GridEngine' Active Grid Interaction id)
     * @see GridEngine' ActiveGridInteraction table
     */
    public int getTargetId() {
        return cmdTargetId;
    }

    /**
     * Set APIServerCommand 'cmdTargetId' field value.
     * 
     * @param target_id
     */
    public void setTargetId(int targetId) {
        if (cmdTargetId != targetId) {
            modifyFlag = true;
            cmdTargetId = targetId;
        }
    }

    /**
     * Get APIServerCommand 'cmdTargetStatus' field value.
     * 
     * @return cmdTargetStatus
     */
    public String getTargetStatus() {
        return cmdTargetStatus;
    }

    /**
     * Set APIServerCommand 'cmdTargetStatus' field value.
     * 
     * @param targetStatus
     */
    public void setTargetStatus(String targetStatus) {
        if (targetStatus == null) {
            return;
        }

        if ((cmdTargetStatus == null)
          || !cmdTargetStatus.equals(targetStatus)) {
            modifyFlag = true;
            cmdTargetStatus = targetStatus;
        }
    }

    /**
     * Get APIServerCommand 'cmdTaskId' field value.
     * 
     * @return cmdTaskId
     */
    public int getTaskId() {
        return cmdTaskId;
    }

    /**
     * Set APIServerCommand 'cmdTaskId' field value.
     * @param taskId
     */
    public void setTaskId(int taskId) {
        if (this.cmdTaskId != cmdTaskId) {
            modifyFlag = true;
            cmdTaskId = cmdTaskId;
        }
    }
}
