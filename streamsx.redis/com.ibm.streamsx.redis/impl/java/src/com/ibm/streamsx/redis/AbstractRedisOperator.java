/* Generated by Streams Studio: January 22, 2020 at 11:34:22 AM GMT+1 */
package com.ibm.streamsx.redis;


import java.net.URI;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisConnectionException;


/**
 * Class for an operator that receives a tuple and then optionally submits a tuple. 
 * This pattern supports one or more input streams and one or more output streams. 
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li> 
 * <li>process() handles a tuple arriving on an input port 
 * <li>processPuncuation() handles a punctuation mark arriving on an input port 
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time, 
 * such as a request to stop a PE or cancel a job. 
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks, 
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other, 
 * which lead to these methods being called concurrently by different threads.</p> 
 */

@Libraries({"impl/lib/ext/*"})

@PrimitiveOperator(name="Redis", namespace="com.ibm.streamsx.redis",
description="Java Operator Redis")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
public class AbstractRedisOperator extends AbstractOperator {

	public Jedis jedis = null;
	// This parameter specifies the REDIS database url.
	private String redisUrl;

	// This parameter specifies the user's password.
	private String redisPassword;
	// This parameter specifies the user's password.
	private int connectionTimeout;
	// This parameter specifies the redisKey.
	private String redisKey;
	
	protected String keyAttr = null;
	protected String valueAttr = null;


	//Parameter redisUrl
	@Parameter(name = "redisUrl", optional = false, 
			description = "This parameter specifies the database url that REDIS client uses to connect to a database and it must have exactly one value of type rstring. The syntax of redis url is specified by database vendors. For example, redis://<server>:<port>  .\\n\\n"
			+ "  **server**, the domain name or IP address of the REDIS database.\\n\\n"
			+ "  **port**, the TCP/IP server port number that is assigned to the REDIS port.\\n\\n"
			+ " This parameter can be overwritten by the **credentials**."
			)
    public void setRedisUrl(String redisUrl){
    	this.redisUrl = redisUrl;
    }

	//Parameter redisKey
	@Parameter(name = "redisKey", optional = true, 
			description = "This parameter specifies the name REDIS key")
    public void setRedisKey(String redisKey){
    	this.redisKey = redisKey;
    }


	
	//Parameter redisPassword
	@Parameter(name = "redisPassword", optional = true, 
			description = "This optional parameter specifies the password of REDIS database. If the redisPassword parameter is specified, it must have exactly one value of type rstring."
			+ ". This parameter can be overwritten by the **credentials**."
			)
    public void setRedisPort(String redisPassword){
    	this.redisPassword = redisPassword;
    }

	
	//Parameter connectionTimeout
	@Parameter(name = "connectionTimeout", optional = true, 
			description = "This optional parameter specifies the connection timeout for REDIS database. The client will wait for n seconds to get an valid connection. If the connectionTimeout parameter is specified, it must have exactly one value of type INT32 grater than zero. "
				)
    public void setConnectionTimeout(int timeout){
    	this.connectionTimeout = timeout;
    }

	//Parameter keyAttr
	@Parameter(name = "keyAttr", optional = false, 
			description = "This parameter specifies the name of `key` attribute that coming through input stream."
			)
    public void setKeyAttr(String keyAttr){
    	this.keyAttr = keyAttr;
    }


	//Parameter valueAttr
	@Parameter(name = "valueAttr", optional = true, 
			description = "This parameter specifies the name of `value attribute' that coming through input stream.")
    public void setValueAttr(String valueAttr){
    	this.valueAttr = valueAttr;
    }

	
	/**
	 * Creates a connection to REDIS database
	 * @throws URISyntaxException
	 */
	public void createRedisConnection() throws URISyntaxException {
		
//		public Jedis(final String host, final int port, final int connectionTimeout, final int soTimeout,
//			      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
//			      final HostnameVerifier hostnameVerifier) {
	
		if ((jedis !=null)&& (jedis.isConnected()))
		{
			jedis.close();
		}
		
//		System.out.println("createRedisConnection " + redisUrl + " " + connectionTimeout  + " " + redisPassword);
		jedis = new Jedis(new URI(redisUrl), connectionTimeout);
		
		if (redisPassword != null){
			jedis.auth(redisPassword);	 
		}
	    jedis.set("foo", "foo_value");
	    jedis.set("name", "gert");
	    
	    String value = jedis.get("foo");
	    jedis.ping();
//	    System.out.println("createRedisConnection " + value + " " +jedis.ping());
	  }

	
	
	/**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        createRedisConnection();
        // TODO:
        // If needed, insert code to establish connections or resources to communicate an external system or data store.
        // The configuration information for this may come from parameters supplied to the operator invocation, 
        // or external configuration files or a combination of the two.
	}

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
    	// This method is commonly used by source operators. 
    	// Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

      
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// For window markers, punctuate all output ports 
    	super.processPunctuation(stream, mark);
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }
}