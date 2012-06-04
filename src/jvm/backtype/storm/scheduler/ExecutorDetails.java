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

    public Integer getEndTask() {
        return endTask;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof ExecutorDetails)) {
            return false;
        }
        
        ExecutorDetails executor = (ExecutorDetails)other;
        return (this.startTask == executor.startTask) && (this.endTask == executor.endTask);
    }
    
    public int hashCode() {
        return this.startTask + 13 * this.endTask;
    }
}
