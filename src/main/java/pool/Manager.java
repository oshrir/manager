package pool;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {
    //MACROS//
    final private static String AMI_ID = "ami-0ff8a91507f77f867";
    final private static String IAM_ROLE = "dps_ass1_role_v2";
    final private static String JAR_NAME = "dps_ass1_worker.jar";
    final private static String KEYPAIR_NAME = "testKey";

    //received from the first local app (will be a macro in local app)
    private static String bucketName = "";

    private static String manager2WorkersSqsUrl;
    private static String workers2ManagerSqsUrl;

    private static AmazonSQSClient sqs;
    private static AmazonS3Client s3;
    private static AmazonEC2Client ec2;

    private static List<Instance> workers;
    private static Map<String, Task> tasks;

    // to delete - only for local tests
    // private static AWSCredentialsProvider credentialsProvider;

    public static void main(String[] args) throws Exception {
        String local2ManagerSqsUrl = args[0];
        bucketName = args[1];
        workers = new ArrayList<Instance>();
        tasks = new HashMap<String, Task>();

        initAWSServices();

        // create new sqs (manager->workers and workers->manager)
        manager2WorkersSqsUrl = sqs.createQueue("actions" + UUID.randomUUID()).getQueueUrl();
        workers2ManagerSqsUrl = sqs.createQueue("responses" + UUID.randomUUID()).getQueueUrl();

        Thread responsesReceiver = new Thread(new ResponsesReceiver(workers2ManagerSqsUrl, tasks, bucketName, s3, sqs));
        Thread statusChecker = new Thread(new StatusChecker(ec2, workers));

        responsesReceiver.start();
        statusChecker.start();

        ExecutorService pool = Executors.newCachedThreadPool(Executors.defaultThreadFactory());

        while (true) {
            Message message = receiveNewTaskMessage(local2ManagerSqsUrl);
            if (message != null) {
                if (message.getMessageAttributes().get("task_type").equals("terminate")) {
                    sqs.deleteMessage(local2ManagerSqsUrl, message.getReceiptHandle());
                    break;
                }

                pool.execute(new NewTaskProcessor(message.getMessageAttributes(), bucketName, manager2WorkersSqsUrl,
                        workers, tasks, s3, sqs));
                sqs.deleteMessage(local2ManagerSqsUrl, message.getReceiptHandle());
            } // else, keep listening to new msg - the loop continues
        }

        terminate(responsesReceiver, statusChecker);
    }

    private static void initAWSServices() {
        // no need to supply credentials, since temporary credentials are created according to the IAM role
        // to delete - only for local tests
        // credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
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
        s3 = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                //remove      .withCredentials(credentialsProvider)
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();
    }

    private static Message receiveNewTaskMessage(String sqsUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames(Collections.singleton("All"))
                //.withMessageAttributeNames("task_type", "bucket_name", "key", "output_sqs", "n")
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        if (messages.isEmpty())
            return null;
        return messages.get(0);
    }

    private static void terminate(Thread responsesReceiver, Thread statusChecker) {
        // wait for all workers to finish their job
        while (true) {
            boolean done = true;
            for (Map.Entry<String, Task> entry: tasks.entrySet()) {
                if (!entry.getValue().isSent()) {
                    done = false;
                    break;
                }
            }
            if (done) break;
        }

        // the responsesReceiver thread finished sending all summary files - should terminate
        responsesReceiver.interrupt();
        statusChecker.interrupt();

        // terminates the workers
        for (Instance worker : workers) {
            ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(worker.getInstanceId()));
        }

        // delete all queues and shutdown the sqs client
        sqs.deleteQueue(manager2WorkersSqsUrl);
        sqs.deleteQueue(workers2ManagerSqsUrl);
        sqs.shutdown();

        // terminates itself
        ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(EC2MetadataUtils.getInstanceId()));
    }

    protected static void createNewWorkers (int amountOfWorkersToCreate) {
        RunInstancesRequest request = new RunInstancesRequest(AMI_ID, amountOfWorkersToCreate, amountOfWorkersToCreate)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withName(IAM_ROLE))
                .withInstanceType(InstanceType.T2Micro.toString())
                .withUserData(makeScript(JAR_NAME))
                .withKeyName(KEYPAIR_NAME);
        List<Instance> newInstances = ec2.runInstances(request).getReservation().getInstances();
        for(Instance instance : newInstances) {
            ec2.createTags(new CreateTagsRequest()
                    .withResources(instance.getInstanceId())
                    .withTags(new Tag("type", "Worker")));
        }
        workers.addAll(newInstances);
    }

    private static String makeScript (String keyName) {
        String scriptSplit =
                "#! /bin/bash \n" +
                        "wget https://s3.amazonaws.com/" + bucketName + "/" + keyName + " -O ./" + keyName + " \n" +
                        "java -jar " + keyName + " " + manager2WorkersSqsUrl + " " + workers2ManagerSqsUrl + " " +
                        bucketName;
        return new String (Base64.encodeBase64(scriptSplit.getBytes()));
    }
}
