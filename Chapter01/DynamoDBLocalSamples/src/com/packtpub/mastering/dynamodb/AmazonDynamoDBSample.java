package com.packtpub.mastering.dynamodb;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This is a sample Class which demonstrates how to use DynamoDB Local
 * 
 * @author Tanmay_Deshpande
 * 
 */
public class AmazonDynamoDBSample {

	static AmazonDynamoDBClient dynamoDBClient;

	private static void init() throws Exception {
		// Instantiate AWS Client with proper credentials
		dynamoDBClient = new AmazonDynamoDBClient(
				new ClasspathPropertiesFileCredentialsProvider());
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDBClient.setRegion(usWest2);
		// Set DynamoDB Local Endpoint
		dynamoDBClient.setEndpoint("http://localhost:8000");
	}

	public static void main(String[] args) throws Exception {
		init();

		try {
			String tableName = "student_table";
			// Create a table with a primary hash key named 'name'
			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(tableName)
					.withKeySchema(
							new KeySchemaElement().withAttributeName("name")
									.withKeyType(KeyType.HASH))
					.withAttributeDefinitions(
							new AttributeDefinition().withAttributeName("name")
									.withAttributeType(ScalarAttributeType.S))
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(
									1L).withWriteCapacityUnits(1L));
			TableDescription createdTableDescription = dynamoDBClient
					.createTable(createTableRequest).getTableDescription();
			System.out.println("Created Table: " + createdTableDescription);

			// Wait for it to become active
			System.out.println("Waiting for " + tableName
					+ " to become ACTIVE...");
			Tables.waitForTableToBecomeActive(dynamoDBClient, tableName);

			// Describe our new table
			DescribeTableRequest describeTableRequest = new DescribeTableRequest()
					.withTableName(tableName);
			TableDescription tableDescription = dynamoDBClient.describeTable(
					describeTableRequest).getTable();
			System.out.println("Table Description: " + tableDescription);

			// Add an item
			Map<String, AttributeValue> item = newItem("James Bond", 86, "XI");
			PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
			PutItemResult putItemResult = dynamoDBClient
					.putItem(putItemRequest);
			System.out.println("Result: " + putItemResult);

			// Scan items for all values
			ScanRequest scanRequest = new ScanRequest(tableName);
			ScanResult scanResult = dynamoDBClient.scan(scanRequest);
			System.out.println("Result: " + scanResult);

		} catch (AmazonServiceException ase) {
			System.out.println("Error Message:" + ase.getMessage());
		} catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static Map<String, AttributeValue> newItem(String name, int marks,
			String rating) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("name", new AttributeValue(name));
		item.put("marks", new AttributeValue().withN(Integer.toString(marks)));
		item.put("class", new AttributeValue(rating));

		return item;
	}

}