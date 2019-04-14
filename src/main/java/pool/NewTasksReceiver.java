package pool;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NewTasksReceiver implements Runnable {

    private String manager2WorkersSqsUrl;
    private String workers2ManagerSqsUrl;
    private String local2ManagerSqsUrl;
    private Map<String, Task> tasks;
    private String bucketName;
    // to delete!
    // private static AWSCredentialsProvider credentialsProvider;
    private List<Instance> workers;
    private AmazonSQSClient sqs;

    public NewTasksReceiver(String manager2WorkersSqsUrl, String workers2ManagerSqsUrl, String local2ManagerSqsUrl,
                            Map<String, Task> tasks, List<Instance> workers, String bucketName) {
        this.manager2WorkersSqsUrl = manager2WorkersSqsUrl;
        this.workers2ManagerSqsUrl = workers2ManagerSqsUrl;
        this.local2ManagerSqsUrl = local2ManagerSqsUrl;
        this.tasks = tasks;
        this.workers = workers;
        this.bucketName = bucketName;
    }

    public void run() {
        // to delete!
        // credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        sqs = (AmazonSQSClient) AmazonSQSClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();

        ExecutorService pool = Executors.newCachedThreadPool(Executors.defaultThreadFactory());

        while (true) {
            Message message = receiveNewTaskMessage(local2ManagerSqsUrl);
            if (message != null) {
                // check if should terminate
                if (message.getMessageAttributes().get("task_type").equals("terminate")) {
                    sqs.deleteMessage(local2ManagerSqsUrl, message.getReceiptHandle());
                    break;
                }

                pool.execute(new NewTaskProcessor(message.getMessageAttributes(), bucketName, manager2WorkersSqsUrl,
                        workers2ManagerSqsUrl, workers, tasks));
                sqs.deleteMessage(local2ManagerSqsUrl, message.getReceiptHandle());

            } // else, keep listening to new msg - the loop continues
        }
    }

    private Message receiveNewTaskMessage(String sqsUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames("task_type", "bucket_name", "key", "output_sqs", "n")
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        if (messages.isEmpty())
            return null;
        return messages.get(0);
    }
}
