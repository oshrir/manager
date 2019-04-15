package pool;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolManager {
    final private String AMI_ID = "ami-0ff8a91507f77f867";
    final private String IAM_ROLE = "dps_ass1_role_v2";

    //received from the first local app (will be a macro in local app)
    private static String bucketName = "";

    private static String manager2WorkersSqsUrl;
    private static String workers2ManagerSqsUrl;

    private static AmazonSQSClient sqs;
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

        // SETUP - aws services
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

        // create new sqs (manager->workers and workers->manager)
        // TODO - add error handling
        manager2WorkersSqsUrl = sqs.createQueue("actions" + UUID.randomUUID()).getQueueUrl();
        workers2ManagerSqsUrl = sqs.createQueue("responses" + UUID.randomUUID()).getQueueUrl();

        Thread responsesReceiver = new Thread(new ResponsesReceiver(workers2ManagerSqsUrl, tasks, bucketName));
        responsesReceiver.start();

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

        terminate(responsesReceiver);

        // TODO - exception handling according to the specifications for all services
        /*try {

        // add exception handling according to our program
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }*/
    }

    private static Message receiveNewTaskMessage(String sqsUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames("task_type", "bucket_name", "key", "output_sqs", "n")
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        if (messages.isEmpty())
            return null;
        return messages.get(0);
    }

    private static void terminate(Thread responsesReceiver) {
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

    private void statusCheck() {
        DescribeInstanceStatusResult result = ec2.describeInstanceStatus(new DescribeInstanceStatusRequest()
            .withIncludeAllInstances(true));
        List<InstanceStatus> statuses = result.getInstanceStatuses();
        for (InstanceStatus status : statuses) {
            if (!("ok".equals(status.getInstanceStatus().getStatus()) &&
                  "ok".equals(status.getSystemStatus().getStatus()))) {
                String instanceID2Terminate = status.getInstanceId();
                ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(instanceID2Terminate));

                RunInstancesRequest request = new RunInstancesRequest(AMI_ID, 1, 1)
                        .withIamInstanceProfile(new IamInstanceProfileSpecification().withName(IAM_ROLE))
                        .withInstanceType(InstanceType.T2Micro.toString())
                        // TODO - this should be the location of the jar file
                        .withUserData(makeScript("dps_ass1_worker.jar"))
                        .withKeyName("testKey");
                Instance newInstance = ec2.runInstances(request).getReservation().getInstances().get(0);
                ec2.createTags(new CreateTagsRequest()
                        .withResources(newInstance.getInstanceId())
                        .withTags(new Tag("type", "Worker")));
                for (Instance instance : workers) {
                    if (instance.getInstanceId().equals(instanceID2Terminate)) {
                        workers.remove(instance);
                        break;
                    }
                }
                workers.add(newInstance);
            }
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
