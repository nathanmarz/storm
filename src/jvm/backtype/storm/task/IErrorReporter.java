package backtype.storm.task;

public interface IErrorReporter {
    void reportError(Throwable error);
}
