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

import org.apache.log4j.Logger;

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
    
    
}
