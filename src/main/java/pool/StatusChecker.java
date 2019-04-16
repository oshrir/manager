package pool;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import java.util.List;

public class StatusChecker implements Runnable {

    private AmazonEC2Client ec2;
    private List<Instance> workers;

    public StatusChecker(AmazonEC2Client ec2, List<Instance> workers) {
        this.ec2 = ec2;
        this.workers = workers;
    }

    public void run() {
        while(true) {
            DescribeInstanceStatusResult result = ec2.describeInstanceStatus(new DescribeInstanceStatusRequest()
                    .withIncludeAllInstances(true));
            List<InstanceStatus> statuses = result.getInstanceStatuses();
            for (InstanceStatus status : statuses) {
                if (!("ok".equals(status.getInstanceStatus().getStatus()) &&
                        "ok".equals(status.getSystemStatus().getStatus()))) {
                    String instanceID2Terminate = status.getInstanceId();
                    ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(instanceID2Terminate));

                    Manager.createNewWorkers(1);
                    for (Instance instance : workers) {
                        if (instance.getInstanceId().equals(instanceID2Terminate)) {
                            workers.remove(instance);
                            break;
                        }
                    }
                }
            }
            try {
                Thread.sleep(60*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
