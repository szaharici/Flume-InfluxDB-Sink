package com.flumetest.influxdb;




import java.util.Map;

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
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;

public class InfluxSink extends AbstractSink implements Configurable {
	private static final Logger LOG = Logger.getLogger(InfluxSink.class);
    private String url;
    private int batchSize;
    private String database;
    private String username;
    private String password;
    private Boolean influxsource;
    private InfluxDB influxDB;
    private String truncateTimestamp;
    private Boolean metricnamefromfield;
    private String metricnamefromconf;
    private String metricvaluefield;
    private String[] tagsfromfieldslist;
    private String timestampfromfield;
    private String metricnamefield;
    private SinkCounter sinkCounter;
    
    @Override
  	public void configure(Context context) {
	    String host = context.getString("host", "localhost");
	    String port = context.getString("port", "8086");
	    String database = context.getString("database", "flumetest");   
	    int batchSize = context.getInteger("batchSize", 100);
	    String username = context.getString("username","root");
	    String password = context.getString("password","root");
	    Boolean influxsource = context.getBoolean("influxdatafrombody",false);
	    Boolean metricnamefromfield = context.getBoolean("metricnamefromfield",false);
	    String metricnamefromconf = context.getString("metricnamefromconf","flume_metric");
	    String metricnamefield = context.getString("metricnamefield", "flume_metric");
	    String metricvaluefield = context.getString("metrcivaluefromfield","fieldname");
	    String tagsfromfields = context.getString("tagsfromfields","field1,field2");
	    String timestampfromfield = context.getString("timestampfromfield","timestamp");
	    String truncateTimestamp = context.getString("truncateTimestamp","no");
	    String url = "http://"+host+":"+port;
	    this.url = url;
	    this.batchSize = batchSize;
	    this.database = database;
	    this.username = username;
	    this.password = password;
	    this.influxsource = influxsource;
	    this.metricnamefromfield = metricnamefromfield;
	    this.truncateTimestamp = truncateTimestamp;
	    this.metricnamefromconf = metricnamefromconf;
	    this.metricvaluefield = metricvaluefield;
	    this.tagsfromfieldslist = tagsfromfields.split(",");
	    this.timestampfromfield = timestampfromfield;
	    this.metricnamefield = metricnamefield;
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
	    		txn.commit();
	    	} 
	    	else {
	    		try {
	    	        LOG.info(batch.toString());
	    			influxDB.write(database, "", InfluxDB.ConsistencyLevel.ONE, batch.toString());
	    			txn.commit();
	     		if ( count < batchSize ) {
	    	    		sinkCounter.incrementBatchUnderflowCount();
	    	    	}
	     			sinkCounter.incrementBatchCompleteCount();
	    		}
	    		catch ( Exception e) {
	    			e.printStackTrace();
	    			LOG.info(e.getMessage());
	    			if ( e.getMessage().contains("unable to parse")) {
	    				LOG.info("This contains bogus data that InfluxDB will never accept. Silently dropping events in order not to fill up channels");
	    				txn.commit();
	    			}
	    			else {
	    			txn.rollback();
	    			}
	    			status = Status.BACKOFF;
	    			sinkCounter.incrementConnectionFailedCount();
	    		}
	    	}
	    	
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

private String ExtractInfluxEvent(Event event, Boolean influx_source, String truncate_timestamp) {
	
	if ( influx_source ) {
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
	else { 
		//Will construct influxdata from events
		Map<String, String> headers = event.getHeaders();
		String tags ="";
		for (int i = 0; i <= tagsfromfieldslist.length - 1; i++) {
    			if (headers.get(tagsfromfieldslist[i]) != null) {
    		    tags=tags+","+tagsfromfieldslist[i]+"="+headers.get(tagsfromfieldslist[i]);	
    		    }
		}
		if ( metricnamefromfield) {

			return headers.get(metricnamefield)+tags+" value="+headers.get(metricvaluefield)+" "+headers.get(timestampfromfield);
		}
		else {
			return metricnamefromconf+tags+" value="+headers.get(metricvaluefield)+" "+headers.get(timestampfromfield);
		}
	
			
}

}
}
