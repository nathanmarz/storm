package backtype.storm.testing;

import backtype.storm.security.auth.SimpleTransportPlugin;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.HashSet;


public class SingleUserSimpleTransport extends SimpleTransportPlugin {
   @Override
   protected Subject getDefaultSubject() {
       HashSet<Principal> principals = new HashSet<Principal>();
       principals.add(new Principal() {
          public String getName() { return "user"; }
          public String toString() { return "user"; }
       });
       return new Subject(true, principals, new HashSet<Object>(), new HashSet<Object>());
   } 
}
