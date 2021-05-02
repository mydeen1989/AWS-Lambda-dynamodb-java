package rfhconnect.handler;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import rfhconnect.model.ConnectionRequest;
import rfhconnect.model.ConnectionResponse;

import java.util.Date;
import java.util.UUID;

public class RFHConnectHandler  implements RequestHandler<ConnectionRequest, ConnectionResponse> {

    private DynamoDB dynamoDb;
    private String DYNAMODB_TABLE_NAME = "connections";
    private Regions REGION = Regions.US_EAST_1;

    public ConnectionResponse handleRequest(
            ConnectionRequest connectionRequest, Context context) {
        System.out.println("Enter into lambda function");
        this.initDynamoDbClient();
        System.out.println("Connection Type is "+ connectionRequest.getType());
        persistData(connectionRequest);

        ConnectionResponse connectionResponse = new ConnectionResponse();
        connectionResponse.setStatus("Success");
        connectionResponse.setMessage("Saved Successfully!!!");
        System.out.println("Saved Successfully "+ connectionRequest.getType());
        return connectionResponse;
    }

    private PutItemOutcome persistData(ConnectionRequest connectionRequest)
            throws ConditionalCheckFailedException {
        return this.dynamoDb.getTable(DYNAMODB_TABLE_NAME)
                .putItem(
                        new PutItemSpec().withItem(new Item()
                                .withString("connectionId", UUID.randomUUID().toString())
                                .withString("type", connectionRequest.getType())
                                .withString("createdBy", connectionRequest.getCreatedBy())
                                .withString("createdDate" , String.valueOf(new Date()))));
    }

    private void initDynamoDbClient() {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        client.setRegion(Region.getRegion(REGION));
        this.dynamoDb = new DynamoDB(client);
    }
}
