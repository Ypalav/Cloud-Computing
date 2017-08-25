package com.PA3.Client.ClientProgram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Client 
{
	static AmazonDynamoDBClient dynamoDB;
	private static final String AWS_ACCESS_KEY_ID = "AKIAJUBRMZPVPYVAW3WA";
	private static final String AWS_SECRET_ACCESS_KEY = "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";
    public static void main(String[] args) throws Exception 
    {
    	String queueName = "";
		String workLoadFile="";

		int i=0;
		int numOfWorkers=0;
		while (i<args.length) {
			if (args[i].equals("client")) {
				i+=1;
				if(args[i].equals("-s")){
					i+=1;
					queueName=args[i];
					i+=1;
					if(args[i].equals("-w")){
						i+=1;
						workLoadFile=args[i];
						break;
					}
				}
			}
		}
		System.out.println();
		
		BasicAWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
        File filename = new File(workLoadFile);
		BufferedReader br = new BufferedReader(new FileReader(filename));
        
        try 
        {   
            // Create a queue
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
            long start = System.currentTimeMillis();
            // Put messages
            String tasks = br.readLine();
    		int lines=0;
    		while(!(tasks==null))
    		{
    			lines++;
    			sqs.sendMessage(new SendMessageRequest(myQueueUrl, tasks+" "+lines));
    			tasks = br.readLine();
    		}        

    		long end = System.currentTimeMillis();
    		long time = (end-start)/1000;
        } 
        catch (AmazonServiceException ase) 
        {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } 
        catch (AmazonClientException ace) 
        {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        br.close();
    }
}
