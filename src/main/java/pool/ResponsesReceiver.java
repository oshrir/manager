package pool;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponsesReceiver implements Runnable {

    protected String workers2ManagerSqsUrl;
    protected Map<String, Task> tasks;
    protected String bucketName;
    private AmazonS3Client s3;
    private AmazonSQSClient sqs;
    // to delete!
    // private static AWSCredentialsProvider credentialsProvider;

    public ResponsesReceiver(String workers2ManagerSqsUrl, Map<String, Task> tasks, String bucketName) {
        this.workers2ManagerSqsUrl = workers2ManagerSqsUrl;
        this.tasks = tasks;
        this.bucketName = bucketName;
    }

    public void run() {
        // to delete!
        // credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        s3 = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();
        sqs = (AmazonSQSClient) AmazonSQSClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();

        while (true) {
            Message response = receiveResponseMessage(workers2ManagerSqsUrl);
            if (response != null) {
                String lastProcessedTaskID = processResponse(response);
                sqs.deleteMessage(workers2ManagerSqsUrl, response.getReceiptHandle());
                Task lastProcessedTask = tasks.get(lastProcessedTaskID);
                lastProcessedTask.incJobs();
                if (lastProcessedTask.isDone()) {
                    // upload the summary file to s3
                    final String outputKey = lastProcessedTaskID + "_summary.txt";
                    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, outputKey, new File(outputKey))
                            .withCannedAcl(CannedAccessControlList.PublicRead);
                    s3.putObject(putObjectRequest);

                    // send sqs msg to the local computer with s3 location of the sum-file
                    Map<String,MessageAttributeValue> msgAttributes = new HashMap<String, MessageAttributeValue>(){
                        {
                            put("key", new MessageAttributeValue().withDataType("String").withStringValue(outputKey));
                        }};
                    SendMessageRequest sendMsgRequest = new SendMessageRequest(lastProcessedTask.getResponseSqsUrl(), "done task")
                            .withMessageAttributes(msgAttributes);
                    sqs.sendMessage(sendMsgRequest);

                    lastProcessedTask.setSent();
                    // for now it is an infinite loop - should add a condition and split into threads
                }
            }
        }
    }

    private Message receiveResponseMessage(String sqsUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames("task_id")
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        if (messages.isEmpty())
            return null;
        return messages.get(0);
    }

    // processes the response and adds the new file to the appropriate file
    protected static String processResponse (Message response) {
        String taskID = response.getMessageAttributes().get("task_id").getStringValue();
        String newLine = response.getBody();
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            // TODO - what is the correct path?
            File sumFile = new File(taskID + "_summary.txt");
            fw = new FileWriter(sumFile.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);
            bw.write(newLine);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return taskID;
    }
}
