import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.util.HashMap;
import java.util.Map;

public class tester {
    public static void main(String[] args) {
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonS3Client s3 = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        AmazonSQSClient sqs = (AmazonSQSClient) AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        AmazonEC2Client ec2 = (AmazonEC2Client) AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        Map<String,MessageAttributeValue> msgAttributes = new HashMap<String, MessageAttributeValue>(){
            {
                put("task_type", new MessageAttributeValue().withDataType("String").withStringValue("new_task"));
                put("bucket_name", new MessageAttributeValue().withDataType("String").withStringValue("testbucket15993"));
                put("key", new MessageAttributeValue().withDataType("String").withStringValue("sampleInput.txt"));
                put("sqs_url", new MessageAttributeValue().withDataType("String").withStringValue("https://sqs.us-east-1.amazonaws.com/632257081133/responses"));
                put("n", new MessageAttributeValue().withDataType("Number").withStringValue("15"));
            }};
        // send a new sqs msg with the details of the input file
        SendMessageRequest sendMsgRequest = new SendMessageRequest("https://sqs.us-east-1.amazonaws.com/632257081133/testQS", "new task")
                .withMessageAttributes(msgAttributes);
        sqs.sendMessage(sendMsgRequest);
    }
}
