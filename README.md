# streamsx.redis

INITIAL VERSION (Development is in progress)

The `streamsx.redis` toolkit project is an open source project focused on the development of operators and functions that extend IBM Streams ability to interact with REDIS database.

https://redis.io/


It uses the JEDIS JAR libraries to communicate with REDIS database. 
https://github.com/xetorthio/jedis.


To make this project you need: 

apache ant  https://ant.apache.org/ 

and 

apache maven https://maven.apache.org/

```
cd com.ibm.streamsx.redis
ant
```

It downloads the needed JAR libraries in directory:
```
 ./streamsx.redis/com.ibm.streamsx.redis/impl/lib/ext
```
and creates the toolkit.


### Remark
This toolkit implements the NLS feature. Use the guidelines for the message bundle that are described in the [Messages and National Language Support for toolkits](https://github.com/IBMStreams/administration/wiki/Messages-and-National-Language-Support-for-toolkits) document.

To learn more about Streams:

* [IBM Streams on Github](http://ibmstreams.github.io)
* [Introduction to Streams Quick Start Edition](http://ibmstreams.github.io/streamsx.documentation/docs/4.3/qse-intro/)
* [Streams Getting Started Guide](http://ibmstreams.github.io/streamsx.documentation/docs/4.3/qse-getting-started/)
* [StreamsDev](https://developer.ibm.com/streamsdev/)
