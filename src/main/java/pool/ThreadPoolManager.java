package pool;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolManager {
    //received from the first local app (will be a macro in local app)
    private static String bucketName = "";

    private static AmazonS3Client s3;
    private static AmazonSQSClient sqs;
    private static AmazonEC2Client ec2;

    private static String manager2WorkersSqsUrl = "";
    private static String workers2ManagerSqsUrl = "";
    private static String local2ManagerSqsUrl = "";

    private static List<Instance> workers;
    private static Map<String, Task> tasks;

    // to delete - only for local tests
    // private static AWSCredentialsProvider credentialsProvider;

    public static void main(String[] args) throws Exception {
        local2ManagerSqsUrl = args[0];
        bucketName = args[1];
        workers = new ArrayList<Instance>();
        tasks = new HashMap<String, Task>();

        // SETUP - aws services
        // no need to supply credentials, since temporary credentials are created according to the IAM role
        // to delete - only for local tests
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

        // create new sqs (manager->workers and workers->manager)
        // TODO - add error handling
        manager2WorkersSqsUrl = sqs.createQueue(new CreateQueueRequest(
                "actions" + UUID.randomUUID())).getQueueUrl();
        workers2ManagerSqsUrl = sqs.createQueue(new CreateQueueRequest(
                "responses" + UUID.randomUUID())).getQueueUrl();

        NewTasksReceiver newTasksReceiver = new NewTasksReceiver(manager2WorkersSqsUrl, workers2ManagerSqsUrl,
                local2ManagerSqsUrl, tasks, workers, bucketName);
        ResponsesReceiver responsesReceiver = new ResponsesReceiver(workers2ManagerSqsUrl, tasks, bucketName);

        Thread newTasksReceiverThread = new Thread(newTasksReceiver);
        Thread responsesReceiverThread = new Thread(responsesReceiver);

        newTasksReceiverThread.start();
        responsesReceiverThread.start();

        // wait until receiving terminate task - the TasksReceiver Thread should end its job
        newTasksReceiverThread.join();
        terminate(responsesReceiverThread);

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

        // terminates the workers and itself
        for (Instance worker : workers) {
            TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(worker.getInstanceId());
            ec2.terminateInstances(request);
        }

        // TODO - turn off the manager - how?
        ec2.shutdown(); // doesnt do the work - only shuts down the client

        // TODO - delete all queues and maybe shutdown the clients
    }
}
