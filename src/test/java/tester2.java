import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class tester2 {
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

        while (true) {
            String inputURL = "https://sqs.us-east-1.amazonaws.com/632257081133/proceduresa6630a2d-8048-4c57-8b99-ca5c50eb8d8a";
            String outputURL = "https://sqs.us-east-1.amazonaws.com/632257081133/responses1be8e1ae-d789-4ff6-b706-b6292b773e9a";
            Message newPdfTask = receiveNewPDFTaskMessage(inputURL, sqs);
            if (newPdfTask != null) {
                Map<String, MessageAttributeValue> msgAttributes = newPdfTask.getMessageAttributes();
                String proc = msgAttributes.get("action").getStringValue();
                String url = msgAttributes.get("file_url").getStringValue();
                final String taskID = msgAttributes.get("task_id").getStringValue();

                sqs.deleteMessage(inputURL, newPdfTask.getReceiptHandle());

                Map<String,MessageAttributeValue> newMsgAttributes = new HashMap<String, MessageAttributeValue>(){
                    {
                        put("task_id", new MessageAttributeValue().withDataType("String").withStringValue(taskID));
                    }};
                // send a new sqs msg with the details of the file
                SendMessageRequest sendMsgRequest = new SendMessageRequest(outputURL, proc + ":\t" + url + " " + url +"\n")
                        .withMessageAttributes(newMsgAttributes);
                sqs.sendMessage(sendMsgRequest);
            }
        }
    }

    private static Message receiveNewPDFTaskMessage(String sqsURL, AmazonSQSClient sqs) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames("procedure", "file_url", "task_id")
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        if (messages.isEmpty())
            return null;
        return messages.get(0);
    }
}
