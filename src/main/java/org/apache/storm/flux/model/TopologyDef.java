package org.apache.storm.flux.model;

import java.util.List;

public class TopologyDef {
    private List<SpoutDef> spouts;

    private List<BoltDef> bolts;

    private List<StreamDef> streams;
}
