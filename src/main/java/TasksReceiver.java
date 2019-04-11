import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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

public class TasksReceiver implements Runnable {

    private String manager2WorkersSqsUrl;
    private String workers2ManagerSqsUrl;
    private String local2ManagerSqsUrl;
    private Map<String, Task> tasks;
    private String bucketName;
    private int tasksCounter = 0;
    // to delete!
    // private static AWSCredentialsProvider credentialsProvider;
    private List<Instance> workers;
    private AmazonS3Client s3;
    private AmazonSQSClient sqs;
    private AmazonEC2Client ec2;

    public TasksReceiver(String manager2WorkersSqsUrl, String workers2ManagerSqsUrl, String local2ManagerSqsUrl,
                         Map<String, Task> tasks, List<Instance> workers, String bucketName) {
        this.manager2WorkersSqsUrl = manager2WorkersSqsUrl;
        this.workers2ManagerSqsUrl = workers2ManagerSqsUrl;
        this.local2ManagerSqsUrl = local2ManagerSqsUrl;
        this.tasks = tasks;
        tasksCounter = 0;
        this.workers = workers;
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
        ec2 = (AmazonEC2Client) AmazonEC2ClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();

        while (true) {
            Message message = receiveNewTaskMessage(local2ManagerSqsUrl);
            if (message != null) {
                // process the msg
                Map<String, MessageAttributeValue> msgAttributes = message.getMessageAttributes();
                String taskType = msgAttributes.get("task_type").getStringValue();
                if (taskType.equals("new_task")) {
                    String key = msgAttributes.get("key").getStringValue();
                    String responseSqsUrl = msgAttributes.get("output_sqs").getStringValue();
                    int messagePerWorker = Integer.parseInt(msgAttributes.get("n").getStringValue());

                    sqs.deleteMessage(local2ManagerSqsUrl, message.getReceiptHandle());

                    String taskID = "task" + tasksCounter;
                    int newMsgsCounter;
                    try {
                        newMsgsCounter = processNewTask(key, taskID);
                        Task newTask = new Task(responseSqsUrl, newMsgsCounter);
                        tasks.put(taskID, newTask);
                        createWorkersIfNeeded(newMsgsCounter / messagePerWorker);

                        assert new File(taskID + "_summary.txt").createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else if (taskType.equals("terminate")) break;
            } // else, keep listening to new msg - the loop continues
        }
        // TODO - check if the following lines are necessary
        s3.shutdown();
        ec2.shutdown();
        sqs.shutdown();
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

    // create the needed number of new instances (TODO - insert bucket and key)
    private void createWorkersIfNeeded(int numOfNeededWorkers) {

        int amountOfWorkersToCreate = numOfNeededWorkers - workers.size();

        if( amountOfWorkersToCreate > 0 ) {
            // TODO - which ami? bucket and key? create a new keyPair?
            RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", amountOfWorkersToCreate, amountOfWorkersToCreate)
                    .withIamInstanceProfile(new IamInstanceProfileSpecification().withName("dps_ass1_role_v2"))
                    //.withIamInstanceProfile(new IamInstanceProfileSpecification().withName("Manager"))
                    .withInstanceType(InstanceType.T2Micro.toString())
                    // TODO - this should be the location of the jar file
                    .withUserData(makeScript(bucketName, "dps_ass1_worker.jar"))
                    .withKeyName("testKey");
                    //.withKeyName("oshrir");
            List<Instance> newInstances = ec2.runInstances(request).getReservation().getInstances();
            for(Instance instance : newInstances) {
                CreateTagsRequest createTagsRequest = new CreateTagsRequest()
                        .withResources(instance.getInstanceId())
                        .withTags(new Tag("type", "Worker"));
                ec2.createTags(createTagsRequest);
            }
            workers.addAll(newInstances);
        }
    }

    private String makeScript (String bucketName, String keyName) {
        String scriptSplit =
                "#! /bin/bash \n" +
                        "wget https://s3.amazonaws.com/" + bucketName + "/" + keyName + " -O ./" + keyName + " \n" +
                        "java -jar " + keyName + " " + manager2WorkersSqsUrl + " " + workers2ManagerSqsUrl + " " +
                        bucketName;
        return new String (Base64.encodeBase64(scriptSplit.getBytes()));
    }
}
