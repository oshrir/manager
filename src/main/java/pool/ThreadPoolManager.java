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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolManager {
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

/*    private void statusCheck() {
        DescribeInstancesResult result = ec2.describeInstances(new DescribeInstancesRequest()
                .withFilters(new Filter("tag:type", new ArrayList<String>(Collections.singletonList("Worker")))));
        for (Reservation reservation : result.getReservations()) {
            List<Instance> instances = reservation.getInstances();
            if (!instances.isEmpty()) {
                for (Instance instance : instances) {
                    instance.
                }
            }
        }
    }*/
}
