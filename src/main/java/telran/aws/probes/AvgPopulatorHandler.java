package telran.aws.probes;

import java.util.Map;

import org.json.simple.parser.JSONParser;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;



public class AvgPopulatorHandler implements RequestHandler<KinesisEvent, String> {
	LambdaLogger log;
	AmazonDynamoDB client ;
	DynamoDB dynamo;
	Table table ;
	String tableName;
	public AvgPopulatorHandler() {
		tableName = System.getenv("TABLE_NAME");
		setDynamoDB();
	}
	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		log = context.getLogger();
		log.log("tableName is " + tableName);
		try {
			input.getRecords().stream().map(this::toProbeDataJSON)
			.forEach(this::probeDataPopulation);
		} catch (Exception e) {
			log.log("ERROR: " + e);
		}
		return null;
	}
	String toProbeDataJSON(KinesisEventRecord record) {
		String recordStr = new String(record.getKinesis().getData().array());
		log.log("received string:  " + recordStr);
		int index = recordStr.indexOf('{');
		if(index < 0) {
			throw new RuntimeException("not found JSON");
		}
		return recordStr.substring(index);
		
	}
	void probeDataPopulation(String probeDataJSON) {
		log.log(String.format("received probeData: %s", probeDataJSON));
		Map<String, Object> mapItem = getMap(probeDataJSON);
		table.putItem(new PutItemSpec().withItem(Item.fromMap(mapItem)));
		log.log(String.format("item %s has been saved to Database", mapItem));
	}
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap(String probeDataJSON) {
		JSONParser parser = new JSONParser();
		Map<String,Object> mapResult = null;
		try {
			mapResult = (Map<String, Object>) parser.parse(probeDataJSON);
		} catch (Exception e) {
			log.log("ERROR JSON parsing " + e);
		}
		return mapResult;
	}
	
	void setDynamoDB() {
		client = AmazonDynamoDBClientBuilder.defaultClient();
		dynamo = new DynamoDB(client);
		table = dynamo.getTable(tableName);
	}

}
