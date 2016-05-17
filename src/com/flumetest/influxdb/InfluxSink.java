package com.flumetest.influxdb;




import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxSink extends AbstractSink implements Configurable {
	private static final Logger LOG = Logger.getLogger(InfluxSink.class);
    private String url;
    private int batchSize;
    private String database;
    private String username;
    private String password;
    private String influxsource;
    private InfluxDB influxDB;
    private String truncateTimestamp;
    private SinkCounter sinkCounter;
    
    @Override
  	public void configure(Context context) {
	    String host = context.getString("host", "localhost");
	    String port = context.getString("port", "8086");
	    String database = context.getString("database", "flumetest");   
	    int batchSize = context.getInteger("batchSize", 100);
	    String username = context.getString("username","root");
	    String password = context.getString("password","root");
	    String influxsource = context.getString("influxsource","body");
	    String truncateTimestamp = context.getString("truncateTimestamp","no");
	    String url = "http://"+host+":"+port;
	    this.url = url;
	    this.batchSize = batchSize;
	    this.database = database;
	    this.username = username;
	    this.password = password;
	    this.influxsource = influxsource;
	    this.truncateTimestamp = truncateTimestamp;
	    if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
    }

    @Override
  	public void start() {
	  LOG.info("Starting Influx Sink {} ...Connecting to "+url);
	  try {
    	InfluxDB influxDB = InfluxDBFactory.connect(url,username,password);
    	this.influxDB = influxDB;
    	sinkCounter.incrementConnectionCreatedCount();
      }
	  
	  catch ( Throwable e ){
    	LOG.error(e.getStackTrace());
    	sinkCounter.incrementConnectionFailedCount();
      }
	  sinkCounter.start();
    }

    @Override
    public void stop () {
    	LOG.info("Stopping Influx Sink {} ...");
    	sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
    	Status status = null;
	    // Start transaction
	    Channel ch = getChannel();
	    Transaction txn = ch.getTransaction();
	    txn.begin();
	    try {
	    	StringBuilder batch = new StringBuilder();
	    	Event event = null;
	    	int count = 0;
	    	sinkCounter.incrementEventDrainAttemptCount();
	    	for (count = 0; count <= batchSize; ++count) {
	    		event = ch.take();
	    		if (event == null) {
	    			break;
	    		}
	    		String InfluxEvent = ExtractInfluxEvent(event, influxsource, truncateTimestamp);
	    		if ( batch.length() > 0) {
	    			batch.append("\n");
	    		}
	    		batch.append(InfluxEvent);
	    		sinkCounter.incrementConnectionCreatedCount();
          
	    	}
	    	if (count <= 0) {
	    		sinkCounter.incrementBatchEmptyCount();
	    		sinkCounter.incrementEventDrainSuccessCount();
	    		status = Status.BACKOFF;
	    	} 
	    	if ( count < batchSize ) {
	    		sinkCounter.incrementBatchUnderflowCount();
	    	}
	    	else {
	    		try {
	    		
	    			influxDB.write(database, "default", InfluxDB.ConsistencyLevel.ONE, batch.toString());
	     			status = Status.READY;
	     			sinkCounter.incrementBatchCompleteCount();
	    		}
	    		catch ( Exception e) {
	    			e.printStackTrace();
	    			LOG.info(e.getMessage());
	    			//txn.rollback();
	    			status = Status.BACKOFF;
	    			sinkCounter.incrementConnectionFailedCount();
	    		}
	    	}
	    	txn.commit();
	    	if(event == null) {
	    		status = Status.BACKOFF;
	    	}

	    	return status;
	    }
	    catch (Throwable t) {
	    	txn.rollback();
	    	// Log exception, handle individual exceptions as needed
	    	LOG.info(t.getMessage());
	    	status = Status.BACKOFF;

	    	// re-throw all Errors
	    	if (t instanceof Error) {
	    		throw (Error)t;
	    	}
	    }
	    finally {
	    	txn.close();
	    }
	    return status;
  }

private String ExtractInfluxEvent(Event event, String influx_source, String truncate_timestamp) {
	
	if ( influx_source.equals("body")) {
        String body = new String(event.getBody());
        //Fix for data coming from windows
        body = body.replaceAll("\\r","");
       
        
        if ( truncate_timestamp.equals("yes")) {
        	
        	//Extract timestamp from message
        	String timestamp = body.substring(body.lastIndexOf(" ")+1);
        	String newtimestamp = timestamp.substring(0,11);
        	body = body.replaceAll(timestamp, newtimestamp);
        	
        }
        return body.toString();

	}
	else { LOG.error("Just body is supported for the moment");
	return null;
	}
			
}

}
