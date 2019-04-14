package pool;

public class Task {
    private String responseSqsUrl;
    private int totalJobs;
    private int doneJobs;
    private boolean sent;

    public Task(String responseSqsUrl, int totalJobs) {
        this.responseSqsUrl = responseSqsUrl;
        this.totalJobs = totalJobs;
        doneJobs = 0;
        sent = false;
    }

    public String getResponseSqsUrl() {
        return responseSqsUrl;
    }

    public void incJobs() {
        doneJobs++;
    }

    public boolean isDone() {
        return totalJobs == doneJobs;
    }

    public void setSent() { sent = true; }

    public boolean isSent() { return sent; }
}
