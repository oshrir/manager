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
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NewTaskProcessor implements Runnable {
    private String bucketName;

    private String manager2LocalSqsUrl;
    private String manager2WorkersSqsUrl;
    private String key;
    private String taskID;
    private int messagePerWorker;
    private AmazonSQSClient sqs;
    private AmazonS3Client s3;
    private List<Instance> workers;
    private Map<String, Task> tasks;

    public NewTaskProcessor(Map<String, MessageAttributeValue> msgAttributes, String bucketName,
                            String manager2WorkersSqsUrl, List<Instance> workers, Map<String, Task> tasks,
                            AmazonS3Client s3, AmazonSQSClient sqs) {
        this.bucketName = bucketName;
        this.manager2WorkersSqsUrl = manager2WorkersSqsUrl;
        manager2LocalSqsUrl = msgAttributes.get("output_sqs").getStringValue();
        key = msgAttributes.get("key").getStringValue();
        messagePerWorker = Integer.parseInt(msgAttributes.get("n").getStringValue());
        taskID = "task" + UUID.randomUUID();
        this.workers = workers;
        this.tasks = tasks;
        this.s3 = s3;
        this.sqs = sqs;
    }

    public void run() {

        int newMsgsCounter;
        try {
            newMsgsCounter = processNewTask(key, taskID);
            tasks.put(taskID, new Task(manager2LocalSqsUrl, newMsgsCounter));

            createWorkersIfNeeded(newMsgsCounter / messagePerWorker);

            assert new File(taskID + "_summary.txt").createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // process the new task, create sqs msgs, returns the number of tasks created
    private int processNewTask (String key, final String taskID) throws IOException {
        S3Object input = s3.getObject(new GetObjectRequest(bucketName, key));
        BufferedReader reader = new BufferedReader(new InputStreamReader(input.getObjectContent()));
        int counter = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            // line is in format: <procedure> <url>
            final String[] taskDetails = line.split("\\s+");
            Map<String,MessageAttributeValue> msgAttributes = new HashMap<String, MessageAttributeValue>(){
                {
                    put("action", new MessageAttributeValue().withDataType("String").withStringValue(taskDetails[0]));
                    put("file_url", new MessageAttributeValue().withDataType("String").withStringValue(taskDetails[1]));
                    put("task_id", new MessageAttributeValue().withDataType("String").withStringValue(taskID));
                }};
            // send a new sqs msg with the details of the file
            SendMessageRequest sendMsgRequest = new SendMessageRequest(manager2WorkersSqsUrl, "new PDF task")
                    .withMessageAttributes(msgAttributes);
            sqs.sendMessage(sendMsgRequest);
            counter++;
        }
        return counter;
    }

    // create the needed number of new instances
    private void createWorkersIfNeeded(int numOfNeededWorkers) {
        // consider the AWS limitation for amount of new instances
        int revisedNumOfNeededWorkers = (numOfNeededWorkers > 19) ? 19 : numOfNeededWorkers;
        int amountOfWorkersToCreate = revisedNumOfNeededWorkers - workers.size();
        if (amountOfWorkersToCreate > 0) {
            Manager.createNewWorkers(amountOfWorkersToCreate);
        }
    }
}
