/*******************************************************************************
 * Copyright (C) 2015-2018 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.state.Checkpoint;
//import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streams.operator.state.StateHandler;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisConnectionException;


@Libraries({"impl/lib/ext/*"})

/**
 * AbstractREDISOperator provides the base class for all REDIS operators.
 */
//public abstract class AbstractREDISOperator extends AbstractOperator implements StateHandler{
	public abstract class AbstractREDISOperator extends AbstractOperator {

	private static final String PACKAGE_NAME = "com.ibm.streamsx.redis";
	private static final String CLASS_NAME = "com.ibm.streamsx.redis.AbstractREDISOperator";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY
		+ "." + CLASS_NAME); 

	// logger for trace/debug information
	protected static Logger TRACE = Logger.getLogger(PACKAGE_NAME);

	public Jedis jedis = null;
	/**
	 * Define operator parameters 
	 */
	// This parameter specifies the path and the filename of redis driver libraries in one comma separated string).
	private String redisHost;
	// This parameter specifies the path and the filename of redis driver libraries in one comma separated string).
	private int redisPort = Protocol.DEFAULT_PORT;
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	
	// This parameter specifies the class name for redis driver.
	private String redisClassName;
	// This parameter specifies the database url.
	private String redisUrl;
	// This parameter specifies the database user on whose behalf the connection is being made.
	private String redisUser;
	// This parameter specifies the user's password.
	private String redisPassword;
	// This parameter specifies the path name of the file that contains the redis connection properties.
	private String redisProperties;
	// This parameter specifies the json string that contains the redis credentials username, password, redisurl.
	private String credentials;
	// This parameter specifies the transaction isolation level at which statement runs.
	// If omitted, the statement runs at level READ_UNCOMMITTED
	private String isolationLevel = IREDISConstants.TRANSACTION_READ_UNCOMMITTED;
	// This parameter specifies the actions when SQL failure.
	protected String sqlFailureAction = IREDISConstants.SQLFAILURE_ACTION_LOG;
	// This optional parameter reconnectionPolicy specifies the reconnection policy
	// that would be applicable during initial/intermittent connection failures.
	// The valid values for this parameter are NoRetry, BoundedRetry and InfiniteRetry.
	// If not specified, it is set to BoundedRetry.
	private String reconnectionPolicy = IREDISConstants.RECONNPOLICY_BOUNDEDRETRY;
	// This optional parameter reconnectionBound specifies the number of successive connection
	// that will be attempted for this operator.
	// It can appear only when the reconnectionPolicy parameter is set to BoundedRetry
	// and cannot appear otherwise.
	// If not present the default value is 5
	private int reconnectionBound = IREDISConstants.RECONN_BOUND_DEFAULT;
	// This optional parameter reconnectionInterval specifies the time period in seconds which
	// the operator will be wait before trying to reconnect.
	// If not specified, the default value is 10.0.
	private double reconnectionInterval = IREDISConstants.RECONN_INTERVAL_DEFAULT;
	private String pluginName = null;
	private int securityMechanism = -1;

	// Create an instance of REDISConnectionhelper
	//protected REDISClientHelper redisClientHelper;

	// Lock (fair mode) for REDIS connection reset
//	private ReadWriteLock lock = new ReentrantReadWriteLock(true);

	// consistent region context
 //   	protected ConsistentRegionContext consistentRegionContext;
    
	// The name of the application config object
	private String appConfigName = null;

	// data from application config object
    	Map<String, String> appConfig = null;

    
 	// SSL parameters
 	private String keyStore;
 	private String trustStore;
 	private String keyStoreType = null;
 	private String trustStoreType = null;
 	private String keyStorePassword = null;
 	private String trustStorePassword = null;
 	private boolean sslConnection;

	//Parameter redisHost
	@Parameter(name = "redisHost", optional = false, 
			description = "This required parameter of type rstring specifies the path and the file name of redis driver librarirs with comma separated in one string. It is recommended to set the value of this parameter without slash at begin, like 'opt/db2jcc4.jar'. In this case the SAB file will contain the driver libraries.\\n\\n"
						+ "Please check the documentation of database vendors and download the latest version of redis drivers. ")
    public void setRedisDriverLib(String redisHost){
    	this.redisHost = redisHost;
    }

	
	//Parameter redisPassword
	@Parameter(name = "redisPort", optional = true, 
			description = "This optional parameter specifies the user’s password. If the redisPassword parameter is specified, it must have exactly one value of type rstring. "
			+ ". This parameter can be overwritten by the **credentials** and **redisProperties** parameters."
			)
    public void setRedisPort(int port){
    	this.redisPort = port;
    }

	
	//Parameter redisPassword
	@Parameter(name = "connectionTimeout", optional = true, 
			description = "This optional parameter specifies the user’s password. If the redisPassword parameter is specified, it must have exactly one value of type rstring. "
			+ ". This parameter can be overwritten by the **credentials** and **redisProperties** parameters."
			)
    public void setConnectionTimeout(int timeout){
    	this.connectionTimeout = timeout;
    }

	
	//Parameter redisClassName
	@Parameter(name = "redisClassName", optional = false, 
			description = "This required parameter specifies the class name for redis driver and it must have exactly one value of type rstring.\\n\\n" 
	                     + "The redis class names are defined by database vendors: \\n\\n"
					     + "For example: \\n\\n "
	                     + "**DB2**        com.ibm.db2.jcc.DB2Driver \\n\\n"
					     + "**ORACLE**     oracle.redis.driver.OracleDriver\\n\\n"
	                     + "**PostgreSQL** org.postgresql.Driver")
    public void setRedisClassName(String redisClassName){
    	this.redisClassName = redisClassName;
    }

	//Parameter redisUrl
	@Parameter(name = "redisUrl", optional = true, 
			description = "This parameter specifies the database url that REDIS driver uses to connect to a database and it must have exactly one value of type rstring. The syntax of redis url is specified by database vendors. For example, redis:db2://<server>:<port>/<database>\\n\\n"
			+ "  **redis:db2** indicates that the connection is to a DB2 for z/OS, DB2 for Linux, UNIX, and Windows.\\n\\n"
			+ "  **server**, the domain name or IP address of the data source.\\n\\n"
			+ "  **port**, the TCP/IP server port number that is assigned to the data source.\\n\\n"
			+ "  **database**, a name for the data source.\\n\\n"
			+ " For details about the redisUrl string please check the documentation of database vendors\\n\\n"
			+ " This parameter can be overwritten by the **credentials** and **redisProperties** parameters."
			)
    public void setRedisUrl(String redisUrl){
    	this.redisUrl = redisUrl;
    }

	//Parameter redisUser
	@Parameter(name = "redisUser", optional = true, 
			description = "This optional parameter specifies the database user on whose behalf the connection is being made. If the **redisUser** parameter is specified, it must have exactly one value of type rstring.\\n\\n"
			+ "This parameter can be overwritten by the **credentials** and **redisProperties** parameters."
			)
    public void setRedisUser(String redisUser){
    	this.redisUser = redisUser;
    }

	//Parameter redisPassword
	@Parameter(name = "redisPassword", optional = true, 
			description = "This optional parameter specifies the user’s password. If the redisPassword parameter is specified, it must have exactly one value of type rstring. "
			+ ". This parameter can be overwritten by the **credentials** and **redisProperties** parameters."
			)
    public void setRedisPassword(String redisPassword){
    	this.redisPassword = redisPassword;
    }

	//Parameter redisProperties
	@Parameter(name = "redisProperties", optional = true, 
			description = "This optional parameter specifies the path name of the file that contains the redis connection properties: **user**, **password** and **redisUrl**. \\n\\n "
					+ "It supports also 'username' or 'redisUser' as 'user' and 'redisPassword' as 'password' and 'redisurl' as 'redisUrl'.")
    public void setRedisProperties(String redisProperties){
    	this.redisProperties = redisProperties;
    }

	//Parameter credentials
	@Parameter(name = "credentials", optional = true, 
			description = "This optional parameter specifies the JSON string that contains the redis credentials: **username**, **password** and **redisurl** or **redisUrl**. \\n\\n"
			+ "This parameter can also be specified in an application configuration.")
    public void setcredentials(String credentials){
    	this.credentials = credentials;
    }

	
	//Parameter isolationLevel
	@Parameter(name = "isolationLevel", optional = true, 
			description = "This optional parameter specifies the transaction isolation level at which statement runs. If omitted, the statement runs at level **READ_UNCOMMITTED**.")
    public void setIsolationLevel(String isolationLevel){
    	this.isolationLevel = isolationLevel;
    }

	//Parameter sqlFailureAction
	@Parameter(name = "sqlFailureAction", optional = true, 
			description = "This optional parameter has values of log, rollback and terminate. If not specified, log is assumed. \\n\\n"
					+ "If sqlFailureAction is **log**, the error is logged, and the error condition is cleared. \\n\\n"
					+ "If sqlFailureAction is **rollback**, the error is logged, the transaction rolls back. \\n\\n"
					+ "If sqlFailureAction is **terminate**, the error is logged, the transaction rolls back and the operator terminates.")
    public void setSqlFailureAction(String sqlFailureAction){
    	this.sqlFailureAction = sqlFailureAction;
    }

	//Parameter reconnectionPolicy
	@Parameter(name = "reconnectionPolicy", optional = true, 
			description = "This optional parameter specifies the policy that is used by the operator to handle database connection failures.  The valid values are: **NoRetry**, **InfiniteRetry**, and **BoundedRetry**. \\n\\n"
					    + "The default value is **BoundedRetry**. If **NoRetry** is specified and a database connection failure occurs, the operator does not try to connect to the database again.  \\n\\n"
					    + "The operator shuts down at startup time if the initial connection attempt fails. If **BoundedRetry** is specified and a database connection failure occurs, the operator tries to connect to the database again up to a maximum number of times. \\n\\n"
					    + "The maximum number of connection attempts is specified in the **reconnectionBound** parameter.  The sequence of connection attempts occurs at startup time. If a connection does not exist, the sequence of connection attempts also occurs before each operator is run. \\n\\n"
					    + "If **InfiniteRetry** is specified, the operator continues to try and connect indefinitely until a connection is made.  This behavior blocks all other operator operations while a connection is not successful.  \\n\\n"
					    + "For example, if an incorrect connection password is specified in the connection configuration document, the operator remains in an infinite startup loop until a shutdown is requested.")
    public void setReconnectionPolicy(String reconnectionPolicy){
    	this.reconnectionPolicy = reconnectionPolicy;
    }

	//Parameter reconnectionBound
	@Parameter(name = "reconnectionBound", optional = true, 
			description = "This optional parameter specifies the number of successive connection attempts that occur when a connection fails or a disconnect occurs.  It is used only when the **reconnectionPolicy** parameter is set to **BoundedRetry**; otherwise, it is ignored. The default value is **5**.")
    public void setReconnectionBound(int reconnectionBound){
    	this.reconnectionBound = reconnectionBound;
    }

	//Parameter reconnectionBound
	@Parameter(name = "reconnectionInterval", optional = true, 
			description = "This optional parameter specifies the amount of time (in seconds) that the operator waits between successive connection attempts.  It is used only when the **reconnectionPolicy** parameter is set to `BoundedRetry` or `InfiniteRetry`; othewise, it is ignored.  The default value is `10`.")
    public void setReconnectionInterval(double reconnectionInterval){
    	this.reconnectionInterval = reconnectionInterval;
    }

	//Parameter sslConnection
	@Parameter(name = "sslConnection", optional = true, 
			description = "This optional parameter specifies whether an SSL connection should be made to the database. When set to `true`, the **keyStore**, **keyStorePassword**, **trustStore** and **trustStorePassword** parameters can be used to specify the locations and passwords of the keyStore and trustStore. The default value is `false`.")
	public void setSslConnection(boolean sslConnection) {
		this.sslConnection = sslConnection;
	}

	public boolean isSslConnection() {
		return sslConnection;
	}

	// Parameter keyStore
	@Parameter(name = "keyStore" , optional = true, 
			description = "This optional parameter specifies the path to the keyStore. If a relative path is specified, the path is relative to the application directory. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	public String getKeyStore() {
		return keyStore;
	}

	// Parameter keyStoreType
	@Parameter(name = "keyStoreType" , optional = true, 
			description = "This optional parameter specifies the type of the keyStore file, for example 'PKCS12'. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setKeyStoreType(String keyStoreType) {
		this.keyStoreType = keyStoreType;
	}

	public String getKeyStoreType() {
		return keyStoreType;
	}
	
	// Parameter trustStoreType
	@Parameter(name = "trustStoreType" , optional = true, 
			description = "This optional parameter specifies the type of the trustStore file, for example 'PKCS12'. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setTrustStoreType(String trustStoreType) {
		this.trustStoreType = trustStoreType;
	}

	public String getTrustStoreType() {
		return trustStoreType;
	}
	
	// Parameter keyStorePassword
	@Parameter(name = "keyStorePassword", optional = true, 
			description = "This parameter specifies the password for the keyStore given by the **keyStore** parameter. The **sslConnection** parameter must be set to `true` for this parameter to have any effect. This parameter can also be specified in an application configuration.")
	public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
	}

	public String getKeyStorePassword() {
		return keyStorePassword;
	}

	// Parameter trustStore
	@Parameter(name = "trustStore", optional = true, 
			description = "This optional parameter specifies the path to the trustStore. If a relative path is specified, the path is relative to the application directory. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setTrustStore(String trustStore) {
		this.trustStore = trustStore;
	}

	public String getTrustStore() {
		return trustStore;
	}

	// Parameter trustStorePassword
	@Parameter(name = "trustStorePassword", optional = true, 
			description = "This parameter specifies the password for the trustStore given by the **trustStore** parameter. The **sslConnection** parameter must be set to `true` for this parameter to have any effect. This parameter can also be specified in an application configuration.")
	public void setTrustStorePassword(String trustStorePassword) {
		this.trustStorePassword = trustStorePassword;
	}

	public String getTrustStorePassword() {
		return trustStorePassword;
	}

	// Parameter appConfigName
	@Parameter(name = "appConfigName", optional = true,
			description = "Specifies the name of the application configuration that contains REDIS connection related configuration parameters. "
			+ " The 'credentials', 'keyStorePassword' and 'trustStorePassword' parameter can be set in an application configuration. "
			+ " If a value is specified in the application configuration and as operator parameter, the application configuration parameter value takes precedence. "
		)
		public void setAppConfigName(String appConfigName) {
			this.appConfigName = appConfigName;
		}	

	// Parameter pluginName
	@Parameter(name = "pluginName", optional = true, description = "Specifies the name of security plugin. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}
	
	// Parameter securityMechanism
	@Parameter(name = "securityMechanism", optional = true, description = "Specifies the value of securityMechanism as Integer. The **sslConnection** parameter must be set to `true` for this parameter to have any effect.")
	public void setSecurityMechanism(int securityMechanism) {
		this.securityMechanism = securityMechanism;
	}
		
	/*
	 * The method checkParametersRuntime
	 */
	@ContextCheck(compile = false)
	public static void checkParametersRuntime(OperatorContextChecker checker) {

		OperatorContext context = checker.getOperatorContext();

		String strReconnectionPolicy = "";
		if (context.getParameterNames().contains("reconnectionPolicy")) {
			// reconnectionPolicy can be either InfiniteRetry, NoRetry,
			// BoundedRetry
			strReconnectionPolicy = context.getParameterValues("reconnectionPolicy").get(0).trim();
			if (!(strReconnectionPolicy.equalsIgnoreCase(IREDISConstants.RECONNPOLICY_NORETRY)
					|| strReconnectionPolicy.equalsIgnoreCase(IREDISConstants.RECONNPOLICY_BOUNDEDRETRY)
				    || strReconnectionPolicy.equalsIgnoreCase(IREDISConstants.RECONNPOLICY_INFINITERETRY))) {
				LOGGER.log(LogLevel.ERROR, "reconnectionPolicy has to be set to InfiniteRetry or NoRetry or BoundedRetry");
				checker.setInvalidContext("reconnectionPolicy has to be set to InfiniteRetry or NoRetry or BoundedRetry", new String[] { context.getParameterValues(
						"reconnectionPolicy").get(0) });
		
			}
		}
		
		// Check reconnection related parameters at runtime
		if ((context.getParameterNames().contains("reconnectionBound"))) {
			// reconnectionBound value should be non negative.
			if (Integer.parseInt(context.getParameterValues("reconnectionBound").get(0)) < 0) {
    			LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_REC_BOUND_NEG")); 
				checker.setInvalidContext(Messages.getString("REDIS_REC_BOUND_NOT_ZERO"), 
						new String[] { context.getParameterValues(
								"reconnectionBound").get(0) });
			}
			if (context.getParameterNames().contains("reconnectionPolicy")) {
				// reconnectionPolicy can be either InfiniteRetry, NoRetry,
				// BoundedRetry
				 strReconnectionPolicy = context.getParameterValues("reconnectionPolicy").get(0).trim();
				// reconnectionBound can appear only when the reconnectionPolicy
				// parameter is set to BoundedRetry and cannot appear otherwise
				if (! strReconnectionPolicy.equalsIgnoreCase(IREDISConstants.RECONNPOLICY_BOUNDEDRETRY)) {
	    			LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_REC_BOUND_NOT_ALLOWED")); 
					checker.setInvalidContext(Messages.getString("REDIS_REC_BOUND_NOT_SET_RETRY"), 
							new String[] { context.getParameterValues(
									"reconnectionBound").get(0) });
				}
			}
		}
		

	}

	
	/*
	 * The method checkParameters
	 */
	@ContextCheck(compile = true)
	public static void checkParameters(OperatorContextChecker checker) {
		// If statement is set as parameter, statementAttr can not be set
		checker.checkExcludedParameters("statement", "statementAttr");
		// If redisProperties is set as parameter, redisUser, redisPassword and redisUrl can not be set
		checker.checkExcludedParameters("redisUser", "redisProperties");
		checker.checkExcludedParameters("redisPassword", "redisProperties");
		checker.checkExcludedParameters("redisUrl", "redisProperties");

		// If credentials is set as parameter, redisUser, redisPassword and redisUrl can not be set.
		checker.checkExcludedParameters("redisUser", "credentials");
		checker.checkExcludedParameters("redisPassword", "credentials");
		checker.checkExcludedParameters("redisUrl", "credentials");
		checker.checkExcludedParameters("credentials", "redisUrl");

		// If credentials is set as parameter, redisProperties can not be set
		checker.checkExcludedParameters("redisProperties", "credentials");
		
		// check reconnection related parameters
		checker.checkDependentParameters("reconnecionInterval", "reconnectionPolicy");
		checker.checkDependentParameters("reconnecionBound", "reconnectionPolicy");

		// check parameters redisUrl redisUser and redisPassword
		OperatorContext context = checker.getOperatorContext();
		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))
				&& (!context.getParameterNames().contains("redisUrl"))
				&& (!context.getParameterNames().contains("redisProperties"))) {
					checker.setInvalidContext(Messages.getString("REDIS_MISSING_REDIS_CRED_PARAM", "redisUrl", "redisUrl"), null);
			}				

		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))				
				&& (!context.getParameterNames().contains("redisProperties"))
				&& (!context.getParameterNames().contains("redisUser"))) {
					checker.setInvalidContext(Messages.getString("REDIS_MISSING_REDIS_CRED_PARAM", "redisUser", "redisUser"), null);
			}				
		if ((!context.getParameterNames().contains("credentials"))
				&& (!context.getParameterNames().contains("appConfigName"))				
				&& (!context.getParameterNames().contains("redisProperties"))
				&& (!context.getParameterNames().contains("redisPassword"))) {
					checker.setInvalidContext(Messages.getString("REDIS_MISSING_REDIS_CRED_PARAM", "redisPassword", "redisPassword"), null);
			}				

	}

	
	@ContextCheck
	public static void checkControlPortInputAttribute(OperatorContextChecker checker) {
		OperatorContext context = checker.getOperatorContext();

		if(context.getNumberOfStreamingInputs() == 2) {
			StreamSchema schema = context.getStreamingInputs().get(1).getStreamSchema();

			//the first attribute must be of type rstring
			Attribute jsonAttr = schema.getAttribute(0);

			//check if the output attribute is present where the result will be stored
			if(jsonAttr != null && jsonAttr.getType().getMetaType() != MetaType.RSTRING) {
				LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_WRONG_CONTROLPORT_TYPE"), jsonAttr.getType()); 
				checker.setInvalidContext();
			}
		}
	}

	
	public void createRedisConnection() throws URISyntaxException {
		
//		public Jedis(final String host, final int port, final int connectionTimeout, final int soTimeout,
//			      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
//			      final HostnameVerifier hostnameVerifier) {
	
		if ((jedis !=null)&& (jedis.isConnected()))
		{
			jedis.close();
		}
		
		System.out.println(" PPPPPPPP " + redisUrl + " " + connectionTimeout + " "  + redisHost + " " + redisPort + " " + redisPassword);
		jedis = new Jedis(new URI(redisUrl), connectionTimeout);
		
//		jedis = new Jedis(redisHost, redisPort, 15000);
		if (redisPassword != null){
			jedis.auth(redisPassword);	 
		}
	    jedis.set("foo", "foo_value");
	    jedis.set("name", "gert");
	    
	    String value = jedis.get("foo");
	    jedis.ping();
	    System.out.println("createRedisConnection CCCC " + value + " " +jedis.ping());
//	    jedis.close();
	  }

	
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
/*
		redisClient = new Connection();
		redisClient.setHost("localhost");
		redisClient.setPort(6379);
		redisClient.connect();
*/
		loadAppConfig(context);

		if (isSslConnection()) {
			if (context.getParameterNames().contains("keyStore")) {
				System.setProperty("javax.net.ssl.keyStore", getAbsolutePath(getKeyStore()));
				if (null != getKeyStoreType()) {
					System.setProperty("javax.net.ssl.keyStoreType", getKeyStoreType());
				}
			}
			if (null != getKeyStorePassword())
				System.setProperty("javax.net.ssl.keyStorePassword", getKeyStorePassword());
			if (context.getParameterNames().contains("trustStore")) {
				System.setProperty("javax.net.ssl.trustStore", getAbsolutePath(getTrustStore()));
				if (null != getTrustStoreType()) {
					System.setProperty("javax.net.ssl.trustStoreType", getTrustStoreType());
				}
			}
			if (null != getTrustStorePassword())
				System.setProperty("javax.net.ssl.trustStorePassword", getTrustStorePassword());
		}
		TRACE.log(TraceLevel.DEBUG," propperties: " + System.getProperties().toString());
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());   //$NON-NLS-3$

		// set up REDIS driver class path
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path...");
//		if(!setupClassPath(context)){
//			TRACE.log(TraceLevel.ERROR, "Operator " + context.getName() + " setting up class path failed.");
//			throw new FileNotFoundException();
//			throw new IOException();
//		}
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up class path - Completed.");

//		consistentRegionContext = context.getOptionalContext(ConsistentRegionContext.class);

		// Create the REDIS connection
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " setting up REDIS connection...");
//		setupREDISConnection();
		createRedisConnection();
		TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " Setting up REDIS connection - Completed");

	}

	/**
	 * read the application config into a map
	 * @param context the operator context 
	 */
	protected void loadAppConfig(OperatorContext context) {
		
		// if no appconfig name is specified, create empty map
		if (appConfigName == null) {
			appConfig = new HashMap<String,String>();
			return;
		}
		
		appConfig = context.getPE().getApplicationConfiguration(appConfigName);
		if (appConfig.isEmpty()) {
			LOGGER.log(LogLevel.WARN, "Application config not found or empty: " + appConfigName);
		}
		
		for (Map.Entry<String, String> kv : appConfig.entrySet()) {
		   	TRACE.log(TraceLevel.DEBUG, "Found application config entry: " + kv.getKey() + "=" + kv.getValue());
		}
		
		if (null != appConfig.get("credentials")){
			credentials = appConfig.get("credentials");
		}
		if (null != appConfig.get("keyStorePassword")){
			keyStorePassword = appConfig.get("keyStorePassword");
		}
		if (null != appConfig.get("trustStorePassword")){
			trustStorePassword = appConfig.get("trustStorePassword");
		}
	}

		
    /**
     * Process an incoming tuple that arrived on the specified port.
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port.
     * </P>
     * @param inputStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {
/*
    	if(inputStream.isControl()) {
    		TRACE.log(TraceLevel.DEBUG, "Process control port...");
			// Acquire write lock to reset the REDIS Connection
    		lock.writeLock().lock();
    		try{
    			processControlPort(inputStream, tuple);
    		}finally{
    			lock.writeLock().unlock();
    		}
			TRACE.log(TraceLevel.DEBUG, "Process control port - Completed");
		}else{
			TRACE.log(TraceLevel.DEBUG, "Process input tuple...");

			// Reset REDIS connection if REDIS connection is not valid
//			if (!redisClientHelper.isConnected()){
				if (!jedis.isConnected()){
	    		TRACE.log(TraceLevel.DEBUG, "REDIS Connection is not valid");
				try {
					// Acquire write lock to reset the REDIS Connection
					lock.writeLock().lock();
					// Reset REDIS connection
					createRedisConnection();
				}finally {
					lock.writeLock().unlock();
				}
				TRACE.log(TraceLevel.DEBUG, "REDIS Connection reset - Completed");
			}

			// Acquire read lock to process SQL statement
			lock.readLock().lock();
			try{
				processTuple(inputStream, tuple);
			}catch (Exception e){
				if((e.toString() != null ) && (e.toString().length() > 0)){
					LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_SQL_EXCEPTION_WARNING", e.toString()), new Object[]{e.toString()}); 
					System.out.println("EEEEEEE2  " + e.toString());
  
				}
        		// Check if REDIS connection valid
//	        	if (redisClientHelper.isValidConnection()){
		        if (!jedis.isConnected()){
	        		// Throw exception for operator to process if REDIS connection is valid
	        		throw e;
	        	}
			}finally{
				lock.readLock().unlock();
			}
			TRACE.log(TraceLevel.DEBUG, "Process input tuple - Completed");
		}
*/
    }

    // Process input tuple
    protected abstract void processTuple (StreamingInput<Tuple> stream, Tuple tuple) throws Exception;

    // Process control port
    // The port allows operator to change REDIS connection information at runtime
    // The port expects a value with JSON format
	protected void processControlPort(StreamingInput<Tuple> stream, Tuple tuple) throws Exception{

		String jsonString = tuple.getString(0);

/*
		try{
			JSONObject redisConnections = JSONObject.parse(jsonString);
			String redisClassName = (String)redisConnections.get("redisClassName");
			String redisUrl = (String)redisConnections.get("redisUrl");
			String redisUser = (String)redisConnections.get("redisUser");
			String redisPassword = (String)redisConnections.get("redisPassword");
			String redisProperties = (String)redisConnections.get("redisProperties");
			String credentials = (String)redisConnections.get("credentials");

			// redisClassName is required
			if (redisClassName == null || redisClassName.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_CLASS_NAME_NOT_EXIST")); 
			}
			// if redisProperties is relative path, convert to absolute path
			if (redisProperties != null && !redisProperties.trim().isEmpty())
			{
				getProperties(redisProperties);
			}

			if (credentials != null && !credentials.trim().isEmpty())
			{
				getCredentials(credentials);
			}

			// redisUrl is required
			if (redisUrl == null || redisUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_URL_NOT_EXIST")); 
			}
			
			// Roll back the transaction
			redisClientHelper.rollbackWithClearBatch();
	        // Reset REDIS connection
			redisClientHelper.resetConnection(redisClassName, redisUrl, redisUser, redisPassword, redisProperties);
		}catch (FileNotFoundException e){
			LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_PROPERTIES_NOT_EXIST"), new Object[]{redisProperties}); 
			throw e;
		}catch (SQLException e){
			LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_RESET_CONNECTION_FAILED"), new Object[]{e.toString()}); 
			throw e;
		}
	*/
	}

    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// Window markers are not forwarded
    	// Window markers are generated on data port (port 0) after a statement
    	// error port (port 1) is punctuation free
		if (mark == Punctuation.FINAL_MARKER) {
			super.processPunctuation(stream, mark);
		}
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();

        TRACE.log(TraceLevel.DEBUG, "Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());   //$NON-NLS-3$
/*
        // Roll back the transaction
        if (sqlFailureAction == "rollback"){
        	redisClientHelper.rollback();
        }
  */      
        // close REDIS connection
               if (jedis.isConnected()){
        	jedis.close();
        }
        
      //  redisClientHelper.closeConnection();

        // Must call super.shutdown()
        super.shutdown();

    }
  
	

	// Set up REDIS connection
	private synchronized void setupREDISConnection() throws Exception{

		// Initiate REDISConnectionHelper instan
        TRACE.log(TraceLevel.DEBUG, "Create REDIS Connection, redisClassName: " + redisClassName);
        TRACE.log(TraceLevel.DEBUG, "Create REDIS Connection, redisUrl: " + redisUrl);
//		try{
			// if redisProperties is relative path, convert to absolute path
			if (redisProperties != null && !redisProperties.trim().isEmpty())
			{
				getProperties(redisProperties);
			}

			if (credentials != null && !credentials.trim().isEmpty()) {
				getCredentials(credentials);
			}
			
			// redisUrl is required
			if (redisUrl == null || redisUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_URL_NOT_EXIST")); 
			}
						
/*			redisClientHelper = new REDISClientHelper(redisClassName, redisUrl, redisUser, redisPassword, sslConnection, redisProperties, isAutoCommit(), isolationLevel, reconnectionPolicy, reconnectionBound, reconnectionInterval, pluginName, securityMechanism);

			redisClientHelper.createConnection();
        }catch (FileNotFoundException e){
        	LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_PROPERTIES_NOT_EXIST"), new Object[]{redisProperties}); 
    		throw e;
    	}catch (SQLException e){
    		LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_CONNECTION_FAILED_ERROR"), new Object[]{e.toString()}); 
		System.out.println("EEEEEEE1");
    		throw e;
    	}
	*/
	}


	// read properties file and set user name, password and redisUrl.
	public void getProperties(String redisProperties) throws IOException {
		try {
				// if redisProperties is relative path, convert to absolute path
				if (!redisProperties.startsWith(File.separator))
				{
					redisProperties = getOperatorContext().getPE().getApplicationDirectory() + File.separator + redisProperties;
				}
				
				System.out.println("REDIS Properties file from Operator '" + getOperatorContext().getName() + "' : " + redisProperties);

				Properties redisConnectionProps = new Properties();
				FileInputStream fileInput = new FileInputStream(redisProperties);
				redisConnectionProps.load(fileInput);
				fileInput.close();

				// It supports 'user' or 'username' or 'redisUser' 			
				redisUser = redisConnectionProps.getProperty("user");
				if (null == redisUser){
						redisUser = redisConnectionProps.getProperty("username");
					}
					if (null == redisUser){
						redisUser = redisConnectionProps.getProperty("redisUser");
						if (null == redisUser){
							LOGGER.log(LogLevel.ERROR, "'user' or 'username' is not defined in property file: " + redisProperties); 
							throw new Exception(Messages.getString("'redisUser' is required to create REDIS connection."));
						}
				}
		        
				// It supports password or redisPassword 			
				redisPassword = redisConnectionProps.getProperty("password");
				if (null == redisPassword){
					redisPassword = redisConnectionProps.getProperty("redisPassword");
					if (null == redisPassword){
						LOGGER.log(LogLevel.ERROR, "'password' or redisPassword' is not defined in property file: " + redisProperties); 
						throw new Exception(Messages.getString("'redisPassword' is required to create REDIS connection."));
					}
				}
                // It supports redisUrl and redisurl 			
				redisUrl = redisConnectionProps.getProperty("redisUrl");
				if (null == redisUrl){
					redisUrl = redisConnectionProps.getProperty("redisurl");
					if (null == redisUrl){
						LOGGER.log(LogLevel.ERROR, "'redisUrl' or 'redisurl' is not defined in property file: " + redisProperties); 
						throw new Exception(Messages.getString("REDIS_URL_NOT_EXIST"));
					}
				}

			} catch (Exception ex) {
				     ex.printStackTrace();
		}
	} 


	// read credentials  and set user name, password and redisUrl.
	public void getCredentials(String credentials) throws IOException {
		String jsonString = credentials;

		try {
			JSONObject obj = JSONObject.parse(jsonString);			
			redisUser = (String)obj.get("username");
			if (redisUser == null || redisUser.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("'redisUser' is required to create REDIS connection.")); 
				throw new Exception(Messages.getString("'redisUser' is required to create REDIS connection."));
			}
		 
			redisPassword = (String)obj.get("password");
			if (redisPassword == null || redisPassword.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("'redisPassword' is required to create REDIS connection.")); 
				throw new Exception(Messages.getString("'redisPassword' is required to create REDIS connection."));
			}
		
			redisUrl = (String)obj.get("redisurl");
			if (redisUrl == null || redisUrl.trim().isEmpty()){
			    redisUrl = (String)obj.get("redisUrl");
			}			
			// redisUrl is required
			if (redisUrl == null || redisUrl.trim().isEmpty()){
				LOGGER.log(LogLevel.ERROR, Messages.getString("REDIS_URL_NOT_EXIST")); 
				throw new Exception(Messages.getString("REDIS_URL_NOT_EXIST"));
			}
			System.out.println("redisUrl from credentials in Operator '" + getOperatorContext().getName() + "' :" + redisUrl);
			
			} catch (Exception ex) {
			         ex.printStackTrace();
		}
	} 
	

	// Reset REDIS connection
	protected void resetREDISConnection() throws Exception{
		// Reset REDIS connection
//		redisClientHelper.resetConnection();

	}
/*
	// REDIS connection need to be auto-committed or not
	protected boolean isAutoCommit(){
        if (consistentRegionContext != null){
        	// Set automatic commit to false when it is a consistent region.
        	return false;
        }
		return true;
	}

	/*


	@Override
	public void close() throws IOException {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_CR_CLOSE")); 
	}

	@Override
	public void checkpoint(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_CR_CHECKPOINT"), checkpoint.getSequenceId()); 

//		redisClientHelper.commit();
	}

	@Override
	public void drain() throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_CR_DRAIN")); 
	}

	@Override
	public void reset(Checkpoint checkpoint) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_CR_RESET"), checkpoint.getSequenceId()); 

//		redisClientHelper.rollback();
	}

	@Override
	public void resetToInitialState() throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_RESET_TO_INITIAL")); 

//		redisClientHelper.rollback();
	}

	@Override
	public void retireCheckpoint(long id) throws Exception {
		LOGGER.log(LogLevel.INFO, Messages.getString("REDIS_CR_RETIRE")); 
	}
*/	
	protected String getAbsolutePath(String filePath) {
		if (filePath == null)
			return null;

		Path p = Paths.get(filePath);
		if (p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File(getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}
	
}
