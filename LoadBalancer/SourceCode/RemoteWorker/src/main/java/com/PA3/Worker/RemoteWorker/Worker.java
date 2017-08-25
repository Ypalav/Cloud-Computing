package com.PA3.Worker.RemoteWorker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
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
//import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Worker {
	static AmazonDynamoDBClient dynamoDB;
	private static final String AWS_ACCESS_KEY_ID = "AKIAJUBRMZPVPYVAW3WA";
	private static final String AWS_SECRET_ACCESS_KEY = "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";

	public static void main(String[] args) throws Exception {
		String queueName = args[0];
		int numOfMessages = Integer.parseInt(args[1]);
		/*
		 * Options options = new Options(); CommandLineParser parser = new
		 * DefaultParser(); options.addOption( "s", true, "Queue Name");
		 * options.addOption( "t", true, "Workload File");
		 * 
		 * try { CommandLine line = parser.parse(options,args);
		 * if(line.hasOption("s") && line.hasOption("t")) { queueName =
		 * line.getOptionValue("s"); } else {
		 * System.out.println("Please Enter the Arguments"); } }
		 * catch(ParseException exp) { System.out.println(
		 * "Unexpected exception:" + exp.getMessage() ); }
		 */

		/*
		 * AWSCredentials credentials = null; try { credentials = new
		 * ProfileCredentialsProvider("default").getCredentials(); } catch
		 * (Exception e) { throw new AmazonClientException(
		 * "Cannot load the credentials from the credential profiles file. " +
		 * "Please make sure that your credentials file is at the correct " +
		 * "location (C:\\Users\\Shorabh\\.aws\\credentials), and is in valid format."
		 * , e); }
		 */
		BasicAWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		dynamoDB = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		dynamoDB.setRegion(usWest2);
		sqs.setRegion(usWest2);
		System.out.println("===========================================");
		System.out.println("WORKER IS RUNNING");
		System.out.println("===========================================\n");

		/*
		 * dynamoDB = new AmazonDynamoDBClient(credentials); Region usEast2 =
		 * Region.getRegion(Regions.US_EAST_1); dynamoDB.setRegion(usEast2);
		 * 
		 * AmazonSQS sqs = new AmazonSQSClient(credentials);
		 */
		try {
			String tableName = "Task_DB";

			// Create table if it does not exist yet

			if (Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is already ACTIVE");
			} else {
				// Create a table with a primary hash key named 'name', which
				// holds a string
				CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
						.withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
						.withAttributeDefinitions(new AttributeDefinition().withAttributeName("name")
								.withAttributeType(ScalarAttributeType.S))
						.withProvisionedThroughput(
								new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
				TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest)
						.getTableDescription();
				System.out.println("Created Table: " + createdTableDescription);

				// Wait for it to become active
				System.out.println("Waiting for " + tableName + " to become ACTIVE...");
				Tables.waitForTableToBecomeActive(dynamoDB, tableName);
				// TableUtils.createTableIfNotExists(dynamoDB,
				// createTableRequest);
				// TableUtils.waitUntilActive(dynamoDB, tableName);
			}
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			String queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

			long start = System.currentTimeMillis();
			// while
			// (!sqs.receiveMessage(receiveMessageRequest).getMessages().isEmpty())
			// {
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			int counter=0;
			while (true) {
				// Receive messages
				System.out.println("Fetch Messages from " + queueName + ".\n");
				receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
				Message currentMessages = sqs.receiveMessage(receiveMessageRequest).getMessages().remove(0);
				
				String body = currentMessages.getBody();
				String messageID = currentMessages.getMessageId();
				String str[] = body.split(" ");

				// Search
				HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
				Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
						.withAttributeValueList(new AttributeValue(str[2]));
				scanFilter.put("name", condition);
				ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
				ScanResult scanResult = dynamoDB.scan(scanRequest);
				// System.out.println("Result: " +
				// scanResult.getItems().isEmpty());

				if (scanResult.getItems().isEmpty()) {
					System.out.println("Message not in DynamoDB");
					Map<String, AttributeValue> item = newItem(str[2], str[0]);
					PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
					PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
					// System.out.println("Result: " + putItemResult);
					Thread.sleep(Long.parseLong(str[1]));
					counter+=1;
				} else {
				}
				// Delete a message
				String messageReceiptHandle = currentMessages.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
				if(counter==numOfMessages){
					break;
				}
			}
			// }
			long end = System.currentTimeMillis();
			long time = (end - start);
			System.out.println("Time Taken By Worker: " + time + " ms.");
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());

			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with SQS, such as not "
					+ "being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static Map<String, AttributeValue> newItem(String name, String value) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("name", new AttributeValue(name));
		item.put("value", new AttributeValue(value));
		return item;
	}
}