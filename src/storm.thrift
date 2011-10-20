#!/usr/local/bin/thrift --gen java:beans,nocamel,hashcode

namespace java backtype.storm.generated

struct NullStruct {
  
}

union Grouping {
  1: list<string> fields; //empty list means global grouping
  2: NullStruct shuffle; // tuple is sent to random task
  3: NullStruct all; // tuple is sent to every task
  4: NullStruct none; // tuple is sent to a single task (storm's choice) -> allows storm to optimize the topology by bundling tasks into a single process
  5: NullStruct direct; // this bolt expects the source bolt to send tuples directly to it
}

struct StreamInfo {
  1: required list<string> output_fields;
  2: required bool direct;
}

struct ShellComponent {
  1: string execution_command;
  2: string script;
}

union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
}

struct ComponentCommon {
  1: required map<i32, StreamInfo> streams; //key is stream id
  2: optional i32 parallelism_hint; //how many threads across the cluster should be dedicated to this component
}

struct SpoutSpec {
  1: required ComponentObject spout_object;
  2: required ComponentCommon common;
  3: required bool distributed;
}

struct GlobalStreamId {
  1: required i32 componentId;
  2: required i32 streamId;
  #Going to need to add an enum for the stream type (NORMAL or FAILURE)
}

struct Bolt {
  1: required map<GlobalStreamId, Grouping> inputs; //a join would have multiple inputs
  2: required ComponentObject bolt_object;
  3: required ComponentCommon common;
}

// not implemented yet
// this will eventually be the basis for subscription implementation in storm
struct StateSpoutSpec {
  1: required ComponentObject state_spout_object;
  2: required ComponentCommon common;
}

struct StormTopology {
  //ids must be unique across maps
  1: required map<i32, SpoutSpec> spouts;
  2: required map<i32, Bolt> bolts;
  3: required map<i32, StateSpoutSpec> state_spouts;
  // #workers to use is in conf
}

exception AlreadyAliveException {
  1: required string msg;
}

exception NotAliveException {
  1: required string msg;
}

exception InvalidTopologyException {
  1: required string msg;
}

struct TopologySummary {
  1: required string id;
  2: required string name;
  3: required i32 num_tasks;
  4: required i32 num_workers;
  5: required i32 uptime_secs;
}

struct SupervisorSummary {
  1: required string host;
  2: required i32 uptime_secs;
  3: required i32 num_workers;
  4: required i32 num_used_workers;  
}

struct ClusterSummary {
  1: required list<SupervisorSummary> supervisors;
  2: required i32 nimbus_uptime_secs;
  3: required list<TopologySummary> topologies;
}

struct ErrorInfo {
  1: required string error;
  2: required i32 error_time_secs;
}

struct BoltStats {
  1: required map<string, map<GlobalStreamId, i64>> acked;  
  2: required map<string, map<GlobalStreamId, i64>> failed;  
  3: required map<string, map<GlobalStreamId, double>> process_ms_avg;
}

struct SpoutStats {
  1: required map<string, map<i32, i64>> acked;
  2: required map<string, map<i32, i64>> failed;
  3: required map<string, map<i32, double>> complete_ms_avg;
}

union TaskSpecificStats {
  1: BoltStats bolt;
  2: SpoutStats spout;
}

// Stats are a map from the time window (all time or a number indicating number of seconds in the window)
//    to the stats. Usually stats are a stream id to a count or average.
struct TaskStats {
  1: required map<string, map<i32, i64>> emitted;
  2: required map<string, map<i32, i64>> transferred;
  3: required TaskSpecificStats specific;
}

struct TaskSummary {
  1: required i32 task_id;
  2: required i32 component_id;
  3: required string host;
  4: required i32 port;
  5: required i32 uptime_secs;
  6: required list<ErrorInfo> errors;
  7: optional TaskStats stats;
}

struct TopologyInfo {
  1: required string id;
  2: required string name;
  3: required i32 uptime_secs;
  4: required list<TaskSummary> tasks;
}

struct KillOptions {
  1: optional i32 wait_secs;
}

service Nimbus {
  void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
  void killTopology(1: string name) throws (1: NotAliveException e);
  void killTopologyWithOpts(1: string name, 2: KillOptions options) throws (1: NotAliveException e);
  // need to add functions for asking about status of storms, what nodes they're running on, looking at task logs

  string beginFileUpload();
  void uploadChunk(1: string location, 2: binary chunk);
  void finishFileUpload(1: string location);
  
  string beginFileDownload(1: string file);
  //can stop downloading chunks when receive 0-length byte array back
  binary downloadChunk(1: string id);
  
  // stats functions
  ClusterSummary getClusterInfo();
  TopologyInfo getTopologyInfo(1: string id) throws (1: NotAliveException e);
  //returns json
  string getTopologyConf(1: string id) throws (1: NotAliveException e);
  StormTopology getTopology(1: string id) throws (1: NotAliveException e);
}

service DistributedRPC {
  string execute(1: string functionName, 2: string funcArgs);
  void result(1: string id, 2: string result);
}
