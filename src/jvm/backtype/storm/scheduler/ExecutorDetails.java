package backtype.storm.scheduler;

public class ExecutorDetails {
    Integer startTask;
    Integer endTask;

    public ExecutorDetails(Integer startTask, Integer endTask){
        this.startTask = startTask;
        this.endTask = endTask;
    }

    public Integer getStartTask() {
        return startTask;
    }

    public void setStartTask(Integer startTask) {
        this.startTask = startTask;
    }

    public Integer getEndTask() {
        return endTask;
    }

    public void setEndTask(Integer endTask) {
        this.endTask = endTask;
    }
}
