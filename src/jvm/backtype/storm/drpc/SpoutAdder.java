package backtype.storm.drpc;


public interface SpoutAdder {
    public void add(String function, String args, String returnInfo);
}
