package storm.trident.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jgrapht.DirectedGraph;
import storm.trident.planner.Node;
import storm.trident.util.IndexedEdge;


public class GraphGrouper {
    
    DirectedGraph<Node, IndexedEdge> graph;
    Set<Group> currGroups;
    Map<Node, Group> groupIndex = new HashMap();
    
    public GraphGrouper(DirectedGraph<Node, IndexedEdge> graph, Collection<Group> initialGroups) {
        this.graph = graph;
        this.currGroups = new HashSet(initialGroups);
        reindex();      
    }
    
    public Collection<Group> getAllGroups() {
        return currGroups;
    }
    
    public void addGroup(Group g) {
        currGroups.add(g);
    }
    
    public void reindex() {
        groupIndex.clear();
        for(Group g: currGroups) {
            for(Node n: g.nodes) {
                groupIndex.put(n, g);
            }
        }  
    }
    
    public void mergeFully() {
        boolean somethingHappened = true;
        while(somethingHappened) {
            somethingHappened = false;
            for(Group g: currGroups) {
                Collection<Group> outgoingGroups = outgoingGroups(g);
                if(outgoingGroups.size()==1) {
                    Group out = outgoingGroups.iterator().next();
                    if(out!=null) {
                        merge(g, out);
                        somethingHappened = true;
                        break;
                    }
                }
                
                Collection<Group> incomingGroups = incomingGroups(g);
                if(incomingGroups.size()==1) {
                    Group in = incomingGroups.iterator().next();
                    if(in!=null) {
                        merge(g, in);
                        somethingHappened = true;
                        break;
                    }
                }                
            }
        }
    }
    
    private void merge(Group g1, Group g2) {
        Group newGroup = new Group(g1, g2);
        currGroups.remove(g1);
        currGroups.remove(g2);
        currGroups.add(newGroup);
        for(Node n: newGroup.nodes) {
            groupIndex.put(n, newGroup);
        }
    }
    
    public Collection<Group> outgoingGroups(Group g) {
        Set<Group> ret = new HashSet();
        for(Node n: g.outgoingNodes()) {
            Group other = nodeGroup(n);
            if(other==null || !other.equals(g)) {
                ret.add(other);                
            }
        }
        return ret;
    }
    
    public Collection<Group> incomingGroups(Group g) {
        Set<Group> ret = new HashSet();
        for(Node n: g.incomingNodes()) {
            Group other = nodeGroup(n);
            if(other==null || !other.equals(g)) {
                ret.add(other);                
            }
        }
        return ret;        
    } 
    
    public Group nodeGroup(Node n) {
        return groupIndex.get(n);
    }
    
}
