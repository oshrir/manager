import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.*;

public class ThreadedManager {
    private static String bucketName = "";

    private static AmazonS3Client s3;
    private static AmazonSQSClient sqs;
    private static AmazonEC2Client ec2;
    private static String manager2WorkersSqsUrl = "";
    private static String workers2ManagerSqsUrl = "";
    private static List<Instance> workers;
    private static Map<String, Task> tasks;
    private static String local2ManagerSqsUrl;
    // to delete!
    // private static AWSCredentialsProvider credentialsProvider;

    public static void main(String[] args) throws Exception {
        local2ManagerSqsUrl = args[0];
        bucketName = args[1];
        tasks = new HashMap<String, Task>();
        workers = new ArrayList<Instance>();

        // SETUP - aws services (TODO - decide whether it is needed here or not)
        // no need to supply credentials, since temporary credentials are created according to the IAM role
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

        // create a new sqs (manager->workers and workers->manager)
        // TODO - add error handling
        manager2WorkersSqsUrl = sqs.createQueue(new CreateQueueRequest(
                "procedures" + UUID.randomUUID())).getQueueUrl();
        workers2ManagerSqsUrl = sqs.createQueue(new CreateQueueRequest(
                "responses" + UUID.randomUUID())).getQueueUrl();

        TasksReceiver tasksReceiver = new TasksReceiver(manager2WorkersSqsUrl, workers2ManagerSqsUrl,
                local2ManagerSqsUrl, tasks, workers, bucketName);
        ResponsesReceiver responsesReceiver = new ResponsesReceiver(workers2ManagerSqsUrl, tasks, bucketName);

        Thread tasksReceiverThread = new Thread(tasksReceiver);
        Thread responsesReceiverThread = new Thread(responsesReceiver);

        tasksReceiverThread.start();
        responsesReceiverThread.start();

        // wait until receiving terminate task - the TasksReceiver Thread should end its job
        tasksReceiverThread.join();
        terminate(responsesReceiverThread);

        // TODO - exception handling according to the specifications
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
                if (!entry.getValue().isDone()) {
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
    }
}