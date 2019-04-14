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

public class ProcessNewTask implements Runnable {

    private String bucketName;
    private String manager2LocalSqsUrl;
    private String manager2WorkersSqsUrl;
    private String workers2ManagerSqsUrl;
    private String key;
    private String taskID;
    private int messagePerWorker;
    private AmazonSQSClient sqs;
    private AmazonS3Client s3;
    private AmazonEC2Client ec2;
    private List<Instance> workers;
    private Map<String, Task> tasks;

    public ProcessNewTask(Map<String, MessageAttributeValue> msgAttributes, String bucketName,
                          String manager2WorkersSqsUrl, String workers2ManagerSqsUrl, List<Instance> workers,
                          Map<String, Task> tasks) {
        this.bucketName = bucketName;
        this.manager2WorkersSqsUrl = manager2WorkersSqsUrl;
        this.workers2ManagerSqsUrl = workers2ManagerSqsUrl;
        manager2LocalSqsUrl = msgAttributes.get("output_sqs").getStringValue();
        key = msgAttributes.get("key").getStringValue();
        messagePerWorker = Integer.parseInt(msgAttributes.get("n").getStringValue());
        taskID = "task" + UUID.randomUUID();
        this.workers = workers;
        this.tasks = tasks;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    public void run() {

        sqs = (AmazonSQSClient) AmazonSQSClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();
        s3 = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();
        ec2 = (AmazonEC2Client) AmazonEC2ClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();

        int newMsgsCounter;
        try {
            newMsgsCounter = processNewTask(key, taskID);
            Task newTask = new Task(manager2LocalSqsUrl, newMsgsCounter);
            tasks.put(taskID, newTask);
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

    // create the needed number of new instances (TODO - insert bucket and key)
    private void createWorkersIfNeeded(int numOfNeededWorkers) {

        int amountOfWorkersToCreate = numOfNeededWorkers - workers.size();

        if( amountOfWorkersToCreate > 0 ) {
            // TODO - which ami? bucket and key? create a new keyPair? AMI should be macro
            RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", amountOfWorkersToCreate, amountOfWorkersToCreate)
                    .withIamInstanceProfile(new IamInstanceProfileSpecification().withName("dps_ass1_role_v2"))
                    //.withIamInstanceProfile(new IamInstanceProfileSpecification().withName("Manager"))
                    .withInstanceType(InstanceType.T2Micro.toString())
                    // TODO - this should be the location of the jar file
                    .withUserData(makeScript("dps_ass1_worker.jar"))
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

    private String makeScript (String keyName) {
        String scriptSplit =
                "#! /bin/bash \n" +
                        "wget https://s3.amazonaws.com/" + bucketName + "/" + keyName + " -O ./" + keyName + " \n" +
                        "java -jar " + keyName + " " + manager2WorkersSqsUrl + " " + workers2ManagerSqsUrl + " " +
                        bucketName;
        return new String (Base64.encodeBase64(scriptSplit.getBytes()));
    }
}
