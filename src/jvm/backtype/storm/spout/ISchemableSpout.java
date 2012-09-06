package backtype.storm.spout;


public interface ISchemableSpout {
     Scheme getScheme();
     void setScheme(Scheme scheme);
}
