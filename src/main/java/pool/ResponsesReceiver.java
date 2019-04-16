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

    private String bucketName;
    private String workers2ManagerSqsUrl;
    private Map<String, Task> tasks;

    private AmazonS3Client s3;
    private AmazonSQSClient sqs;
    // to delete!
    // private static AWSCredentialsProvider credentialsProvider;

    public ResponsesReceiver(String workers2ManagerSqsUrl, Map<String, Task> tasks, String bucketName,
                             AmazonS3Client s3, AmazonSQSClient sqs) {
        this.workers2ManagerSqsUrl = workers2ManagerSqsUrl;
        this.tasks = tasks;
        this.bucketName = bucketName;
        this.s3 = s3;
        this.sqs = sqs;
    }

    public void run() {
        while (true) {
            Message response = receiveResponseMessage(workers2ManagerSqsUrl);
            if (response != null) {
                String lastProcessedTaskID = processResponse(response);
                sqs.deleteMessage(workers2ManagerSqsUrl, response.getReceiptHandle());
                Task lastProcessedTask = tasks.get(lastProcessedTaskID);
                lastProcessedTask.incJobs();
                if (lastProcessedTask.isDone())
                    uploadSummaryToS3AndSendMsg(lastProcessedTaskID, lastProcessedTask);
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
    private static String processResponse (Message response) {
        String taskID = response.getMessageAttributes().get("task_id").getStringValue();
        String newLine = response.getBody();
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            File sumFile = new File(taskID + "_summary.txt");
            fw = new FileWriter(sumFile.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);
            bw.write(newLine);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) bw.close();
                if (fw != null) fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return taskID;
    }

    // upload the summary file to s3 and send sqs msg to the local computer with s3 location of the sum-file
    private void uploadSummaryToS3AndSendMsg (String doneTaskID, Task doneTask) {
        final String outputKey = doneTaskID + "_summary.txt";
        s3.putObject(new PutObjectRequest(bucketName, "output/summary/" + outputKey, new File(outputKey))
                .withCannedAcl(CannedAccessControlList.PublicRead));

        sqs.sendMessage(new SendMessageRequest(doneTask.getResponseSqsUrl(), "done task")
                .withMessageAttributes(new HashMap<String, MessageAttributeValue>(){
                    {
                        put("key", new MessageAttributeValue()
                                .withDataType("String")
                                .withStringValue("output/summary/" + outputKey));
                    }}));
        doneTask.setSent();
    }
}
