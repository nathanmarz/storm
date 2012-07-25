package backtype.storm.scheduler;

public class ExecutorDetails {
    int startTask;
    int endTask;

    public ExecutorDetails(int startTask, int endTask){
        this.startTask = startTask;
        this.endTask = endTask;
    }

    public int getStartTask() {
        return startTask;
    }

    public int getEndTask() {
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
    
    @Override
    public String toString() {
    	return "[" + this.startTask + ", " + this.endTask + "]";
    }
}
