package com.dynamo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// include the following class when using DynamoDB Local
// import com.amazonaws.client.builder.AwsClientBuilder;
// import com.amazonaws.auth.AWSStaticCredentialsProvider;
// import com.amazonaws.auth.BasicAWSCredentials;


public class AmazonDynamoDBPutItem {

    static int counter = 557001;

    public static void main(String[] args) {
        // Create the DynamoDB Client with the region you want
        AmazonDynamoDB dynamoDB = createDynamoDbClient("ap-southeast-2");

        try {
            // Create PutItemRequests
            for (int i = 0; i < 500; i++) {
                PutItemRequest putItemRequest = createPutItemRequest();
                PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
                System.out.println("Successfully put item - " + counter);
                counter++;
            }
            System.out.println("Successfully put all item.");
            // Handle putItemResult

        } catch (Exception e) {
            handlePutItemErrors(e);
        }
    }

    private static AmazonDynamoDB createDynamoDbClient(String region) {
        // Use the following builder when using DynamoDB Local
        /*return AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "localhost"))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials("access_key_id", "secret_access_key"))
                ).build();*/
        return AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
    }

    private static PutItemRequest createPutItemRequest() {
        PutItemRequest putItemRequest = new PutItemRequest();
        putItemRequest.setTableName("BIN");
        putItemRequest.setItem(getItem());
        return putItemRequest;
    }

    private static Map<String, AttributeValue> getItem() {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("bin", new AttributeValue("" + counter));
        item.put("eftposAvailableDate", new AttributeValue("2014-11-01"));
        item.put("binProductType", new AttributeValue("multi-network"));
        item.put("binProductSubtype", new AttributeValue("credit"));
        item.put("lastUpdated", new AttributeValue("2020-07-30"));
        item.put("eftposDigitalAccountLabel", new AttributeValue("cheque"));
        item.put("eftposIssuerName", new AttributeValue("Credit Union Association Limited"));
        item.put("eftposIssuerShortName", new AttributeValue("ACUI"));
        item.put("panLength", new AttributeValue("16"));
        item.put("isTokenizable", new AttributeValue().withBOOL(true));
        item.put("isMerchantChoiceRoutable", new AttributeValue().withBOOL(true));
        item.put("eftposDigitalTransactionTypesAllowed", new AttributeValue().withL(getAttributeValueMethod1()));
        return item;
    }

    private static List<AttributeValue> getAttributeValueMethod1() {
        List<AttributeValue> attributeValues = new ArrayList<AttributeValue>();
        attributeValues.add(new AttributeValue("account-verify"));
        attributeValues.add(new AttributeValue("deposit"));
        attributeValues.add(new AttributeValue("purchase"));
        attributeValues.add(new AttributeValue("refund"));
        attributeValues.add(new AttributeValue("withdrawal"));
        return attributeValues;
    }

    private static void handlePutItemErrors(Exception exception) {
        try {
            throw exception;
        } catch (ConditionalCheckFailedException ccfe) {
            System.out.println("Condition check specified in the operation failed, review and update the condition " +
                    "check before retrying. Error: " + ccfe.getErrorMessage());
        } catch (TransactionConflictException tce) {
            System.out.println("Operation was rejected because there is an ongoing transaction for the item, generally " +
                    "safe to retry with exponential back-off. Error: " + tce.getErrorMessage());
        } catch (ItemCollectionSizeLimitExceededException icslee) {
            System.out.println("An item collection is too large, you're using Local Secondary Index and exceeded " +
                    "size limit of items per partition key. Consider using Global Secondary Index instead. Error: " + icslee.getErrorMessage());
        } catch (Exception e) {
            handleCommonErrors(e);
        }
    }

    private static void handleCommonErrors(Exception exception) {
        try {
            throw exception;
        } catch (InternalServerErrorException isee) {
            System.out.println("Internal Server Error, generally safe to retry with exponential back-off. Error: " + isee.getErrorMessage());
        } catch (RequestLimitExceededException rlee) {
            System.out.println("Throughput exceeds the current throughput limit for your account, increase account level throughput before " +
                    "retrying. Error: " + rlee.getErrorMessage());
        } catch (ProvisionedThroughputExceededException ptee) {
            System.out.println("Request rate is too high. If you're using a custom retry strategy make sure to retry with exponential back-off. " +
                    "Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index. Error: " +
                    ptee.getErrorMessage());
        } catch (ResourceNotFoundException rnfe) {
            System.out.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getErrorMessage());
        } catch (AmazonServiceException ase) {
            System.out.println("An AmazonServiceException occurred, indicates that the request was correctly transmitted to the DynamoDB " +
                    "service, but for some reason, the service was not able to process it, and returned an error response instead. Investigate and " +
                    "configure retry strategy. Error type: " + ase.getErrorType() + ". Error message: " + ase.getErrorMessage());
        } catch (AmazonClientException ace) {
            System.out.println("An AmazonClientException occurred, indicates that the client was unable to get a response from DynamoDB " +
                    "service, or the client was unable to parse the response from the service. Investigate and configure retry strategy. " +
                    "Error: " + ace.getMessage());
        } catch (Exception e) {
            System.out.println("An exception occurred, investigate and configure retry strategy. Error: " + e.getMessage());
        }
    }

}