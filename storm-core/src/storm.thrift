#!/usr/local/bin/thrift --gen java:beans,nocamel,hashcode

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

namespace java backtype.storm.generated

union JavaObjectArg {
  1: i32 int_arg;
  2: i64 long_arg;
  3: string string_arg;
  4: bool bool_arg;
  5: binary binary_arg;
  6: double double_arg;
}

struct JavaObject {
  1: required string full_class_name;
  2: required list<JavaObjectArg> args_list;
}

struct NullStruct {
  
}

struct GlobalStreamId {
  1: required string componentId;
  2: required string streamId;
  #Going to need to add an enum for the stream type (NORMAL or FAILURE)
}

union Grouping {
  1: list<string> fields; //empty list means global grouping
  2: NullStruct shuffle; // tuple is sent to random task
  3: NullStruct all; // tuple is sent to every task
  4: NullStruct none; // tuple is sent to a single task (storm's choice) -> allows storm to optimize the topology by bundling tasks into a single process
  5: NullStruct direct; // this bolt expects the source bolt to send tuples directly to it
  6: JavaObject custom_object;
  7: binary custom_serialized;
  8: NullStruct local_or_shuffle; // prefer sending to tasks in the same worker process, otherwise shuffle
}

struct StreamInfo {
  1: required list<string> output_fields;
  2: required bool direct;
}

struct ShellComponent {
  // should change this to 1: required list<string> execution_command;
  1: string execution_command;
  2: string script;
}

union ComponentObject {
  1: binary serialized_java;
  2: ShellComponent shell;
  3: JavaObject java_object;
}

struct ComponentCommon {
  1: required map<GlobalStreamId, Grouping> inputs;
  2: required map<string, StreamInfo> streams; //key is stream id
  3: optional i32 parallelism_hint; //how many threads across the cluster should be dedicated to this component

  // component specific configuration respects:
  // topology.debug: false
  // topology.max.task.parallelism: null // can replace isDistributed with this
  // topology.max.spout.pending: null
  // topology.kryo.register // this is the only additive one
  
  // component specific configuration
  4: optional string json_conf;
}

struct SpoutSpec {
  1: required ComponentObject spout_object;
  2: required ComponentCommon common;
  // can force a spout to be non-distributed by overriding the component configuration
  // and setting TOPOLOGY_MAX_TASK_PARALLELISM to 1
}

struct Bolt {
  1: required ComponentObject bolt_object;
  2: required ComponentCommon common;
}

// not implemented yet
// this will eventually be the basis for subscription implementation in storm
struct StateSpoutSpec {
  1: required ComponentObject state_spout_object;
  2: required ComponentCommon common;
}

struct StormTopology {
  //ids must be unique across maps
  // #workers to use is in conf
  1: required map<string, SpoutSpec> spouts;
  2: required map<string, Bolt> bolts;
  3: required map<string, StateSpoutSpec> state_spouts;
}

exception AlreadyAliveException {
  1: required string msg;
}

exception NotAliveException {
  1: required string msg;
}

exception AuthorizationException {
  1: required string msg;
}

exception InvalidTopologyException {
  1: required string msg;
}

struct TopologySummary {
  1: required string id;
  2: required string name;
  3: required i32 num_tasks;
  4: required i32 num_executors;
  5: required i32 num_workers;
  6: required i32 uptime_secs;
  7: required string status;
513: optional string sched_status;
514: optional string owner;
515: optional i32 replication_count;
521: optional double requested_memonheap;
522: optional double requested_memoffheap;
523: optional double requested_cpu;
524: optional double assigned_memonheap;
525: optional double assigned_memoffheap;
526: optional double assigned_cpu;
}

struct SupervisorSummary {
  1: required string host;
  2: required i32 uptime_secs;
  3: required i32 num_workers;
  4: required i32 num_used_workers;
  5: required string supervisor_id;
  6: optional string version = "VERSION_NOT_PROVIDED";
  7: optional map<string, double> total_resources;
}

struct NimbusSummary {
  1: required string host;
  2: required i32 port;
  3: required i32 uptime_secs;
  4: required bool isLeader;
  5: required string version;
}

struct ClusterSummary {
  1: required list<SupervisorSummary> supervisors;
  3: required list<TopologySummary> topologies;
  4: required list<NimbusSummary> nimbuses;
}

struct ErrorInfo {
  1: required string error;
  2: required i32 error_time_secs;
  3: optional string host;
  4: optional i32 port;
}

struct BoltStats {
  1: required map<string, map<GlobalStreamId, i64>> acked;  
  2: required map<string, map<GlobalStreamId, i64>> failed;  
  3: required map<string, map<GlobalStreamId, double>> process_ms_avg;
  4: required map<string, map<GlobalStreamId, i64>> executed;  
  5: required map<string, map<GlobalStreamId, double>> execute_ms_avg;
}

struct SpoutStats {
  1: required map<string, map<string, i64>> acked;
  2: required map<string, map<string, i64>> failed;
  3: required map<string, map<string, double>> complete_ms_avg;
}

union ExecutorSpecificStats {
  1: BoltStats bolt;
  2: SpoutStats spout;
}

// Stats are a map from the time window (all time or a number indicating number of seconds in the window)
//    to the stats. Usually stats are a stream id to a count or average.
struct ExecutorStats {
  1: required map<string, map<string, i64>> emitted;
  2: required map<string, map<string, i64>> transferred;
  3: required ExecutorSpecificStats specific;
  4: required double rate;
}

struct ExecutorInfo {
  1: required i32 task_start;
  2: required i32 task_end;
}

struct ExecutorSummary {
  1: required ExecutorInfo executor_info;
  2: required string component_id;
  3: required string host;
  4: required i32 port;
  5: required i32 uptime_secs;
  7: optional ExecutorStats stats;
}

struct TopologyInfo {
  1: required string id;
  2: required string name;
  3: required i32 uptime_secs;
  4: required list<ExecutorSummary> executors;
  5: required string status;
  6: required map<string, list<ErrorInfo>> errors;
  7: optional map<string, DebugOptions> component_debug;
513: optional string sched_status;
514: optional string owner;
515: optional i32 replication_count;
521: optional double requested_memonheap;
522: optional double requested_memoffheap;
523: optional double requested_cpu;
524: optional double assigned_memonheap;
525: optional double assigned_memoffheap;
526: optional double assigned_cpu;
}

struct DebugOptions {
  1: optional bool enable
  2: optional double samplingpct
}

struct CommonAggregateStats {
1: optional i32 num_executors;
2: optional i32 num_tasks;
3: optional i64 emitted;
4: optional i64 transferred;
5: optional i64 acked;
6: optional i64 failed;
}

struct SpoutAggregateStats {
1: optional double complete_latency_ms;
}

struct BoltAggregateStats {
1: optional double execute_latency_ms;
2: optional double process_latency_ms;
3: optional i64    executed;
4: optional double capacity;
}

union SpecificAggregateStats {
1: BoltAggregateStats  bolt;
2: SpoutAggregateStats spout;
}

enum ComponentType {
  BOLT = 1,
  SPOUT = 2
}

struct ComponentAggregateStats {
1: optional ComponentType type;
2: optional CommonAggregateStats common_stats;
3: optional SpecificAggregateStats specific_stats;
4: optional ErrorInfo last_error;
}

struct TopologyStats {
1: optional map<string, i64> window_to_emitted;
2: optional map<string, i64> window_to_transferred;
3: optional map<string, double> window_to_complete_latencies_ms;
4: optional map<string, i64> window_to_acked;
5: optional map<string, i64> window_to_failed;
}

struct TopologyPageInfo {
 1: required string id;
 2: optional string name;
 3: optional i32 uptime_secs;
 4: optional string status;
 5: optional i32 num_tasks;
 6: optional i32 num_workers;
 7: optional i32 num_executors;
 8: optional string topology_conf;
 9: optional map<string,ComponentAggregateStats> id_to_spout_agg_stats;
10: optional map<string,ComponentAggregateStats> id_to_bolt_agg_stats;
11: optional string sched_status;
12: optional TopologyStats topology_stats;
13: optional string owner;
14: optional DebugOptions debug_options;
15: optional i32 replication_count;
521: optional double requested_memonheap;
522: optional double requested_memoffheap;
523: optional double requested_cpu;
524: optional double assigned_memonheap;
525: optional double assigned_memoffheap;
526: optional double assigned_cpu;
}

struct ExecutorAggregateStats {
1: optional ExecutorSummary exec_summary;
2: optional ComponentAggregateStats stats;
}

struct ComponentPageInfo {
 1: required string component_id;
 2: required ComponentType component_type;
 3: optional string topology_id;
 4: optional string topology_name;
 5: optional i32 num_executors;
 6: optional i32 num_tasks;
 7: optional map<string,ComponentAggregateStats> window_to_stats;
 8: optional map<GlobalStreamId,ComponentAggregateStats> gsid_to_input_stats;
 9: optional map<string,ComponentAggregateStats> sid_to_output_stats;
10: optional list<ExecutorAggregateStats> exec_stats;
11: optional list<ErrorInfo> errors;
12: optional string eventlog_host;
13: optional i32 eventlog_port;
14: optional DebugOptions debug_options;
15: optional string topology_status;
}

struct KillOptions {
  1: optional i32 wait_secs;
}

struct RebalanceOptions {
  1: optional i32 wait_secs;
  2: optional i32 num_workers;
  3: optional map<string, i32> num_executors;
}

struct Credentials {
  1: required map<string,string> creds;
}

enum TopologyInitialStatus {
    ACTIVE = 1,
    INACTIVE = 2
}
struct SubmitOptions {
  1: required TopologyInitialStatus initial_status;
  2: optional Credentials creds;
}

struct SupervisorInfo {
    1: required i64 time_secs;
    2: required string hostname;
    3: optional string assignment_id;
    4: optional list<i64> used_ports;
    5: optional list<i64> meta;
    6: optional map<string, string> scheduler_meta;
    7: optional i64 uptime_secs;
    8: optional string version;
    9: optional map<string, double> resources_map;
}

struct NodeInfo {
    1: required string node;
    2: required set<i64> port;
}

struct WorkerResources {
    1: optional double mem_on_heap;
    2: optional double mem_off_heap;
    3: optional double cpu;
}
struct Assignment {
    1: required string master_code_dir;
    2: optional map<string, string> node_host = {};
    3: optional map<list<i64>, NodeInfo> executor_node_port = {};
    4: optional map<list<i64>, i64> executor_start_time_secs = {};
    5: optional map<NodeInfo, WorkerResources> worker_resources = {};
}

enum TopologyStatus {
    ACTIVE = 1,
    INACTIVE = 2,
    REBALANCING = 3,
    KILLED = 4
}

union TopologyActionOptions {
    1: optional KillOptions kill_options;
    2: optional RebalanceOptions rebalance_options;
}

struct StormBase {
    1: required string name;
    2: required TopologyStatus status;
    3: required i32 num_workers;
    4: optional map<string, i32> component_executors;
    5: optional i32 launch_time_secs;
    6: optional string owner;
    7: optional TopologyActionOptions topology_action_options;
    8: optional TopologyStatus prev_status;//currently only used during rebalance action.
    9: optional map<string, DebugOptions> component_debug; // topology/component level debug option.
}

struct ClusterWorkerHeartbeat {
    1: required string storm_id;
    2: required map<ExecutorInfo,ExecutorStats> executor_stats;
    3: required i32 time_secs;
    4: required i32 uptime_secs;
}

struct ThriftSerializedObject {
  1: required string name;
  2: required binary bits;
}

struct LocalStateData {
   1: required map<string, ThriftSerializedObject> serialized_parts;
}

struct LocalAssignment {
  1: required string topology_id;
  2: required list<ExecutorInfo> executors;
  3: optional WorkerResources resources;
}

struct LSSupervisorId {
   1: required string supervisor_id;
}

struct LSApprovedWorkers {
   1: required map<string, i32> approved_workers;
}

struct LSSupervisorAssignments {
   1: required map<i32, LocalAssignment> assignments; 
}

struct LSWorkerHeartbeat {
   1: required i32 time_secs;
   2: required string topology_id;
   3: required list<ExecutorInfo> executors
   4: required i32 port;
}

struct LSTopoHistory {
   1: required string topology_id;
   2: required i64 time_stamp;
   3: required list<string> users;
   4: required list<string> groups;
}

struct LSTopoHistoryList {
  1: required list<LSTopoHistory> topo_history;
}

enum NumErrorsChoice {
  ALL,
  NONE,
  ONE
}

enum ProfileAction {
  JPROFILE_STOP,
  JPROFILE_START,
  JPROFILE_DUMP,
  JMAP_DUMP,
  JSTACK_DUMP,
  JVM_RESTART
}

struct ProfileRequest {
  1: required NodeInfo nodeInfo,
  2: required ProfileAction action,
  3: optional i64 time_stamp; 
}

struct GetInfoOptions {
  1: optional NumErrorsChoice num_err_choice;
}

enum LogLevelAction {
  UNCHANGED = 1,
  UPDATE    = 2,
  REMOVE    = 3
}

struct LogLevel {
  1: required LogLevelAction action;

  // during this thrift call, we'll move logger to target_log_level
  2: optional string target_log_level;

  // number of seconds that target_log_level should be kept
  // after this timeout, the loggers will be reset to reset_log_level
  // if timeout is 0, we will not reset 
  3: optional i32 reset_log_level_timeout_secs;

  // number of seconds since unix epoch corresponding to 
  // current time (when message gets to nimbus) + reset_log_level_timeout_se
  // NOTE: this field gets set in Nimbus 
  4: optional i64 reset_log_level_timeout_epoch;

  // if reset timeout was set, then we would reset 
  // to this level after timeout (or INFO by default)
  5: optional string reset_log_level;
}

struct LogConfig { 
  // logger name -> log level map
  2: optional map<string, LogLevel> named_logger_level;
}

struct TopologyHistoryInfo {
  1: list<string> topo_ids;
}

service Nimbus {
  void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3: AuthorizationException aze);
  void submitTopologyWithOpts(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology, 5: SubmitOptions options) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite, 3: AuthorizationException aze);
  void killTopology(1: string name) throws (1: NotAliveException e, 2: AuthorizationException aze);
  void killTopologyWithOpts(1: string name, 2: KillOptions options) throws (1: NotAliveException e, 2: AuthorizationException aze);
  void activate(1: string name) throws (1: NotAliveException e, 2: AuthorizationException aze);
  void deactivate(1: string name) throws (1: NotAliveException e, 2: AuthorizationException aze);
  void rebalance(1: string name, 2: RebalanceOptions options) throws (1: NotAliveException e, 2: InvalidTopologyException ite, 3: AuthorizationException aze);

  // dynamic log levels
  void setLogConfig(1: string name, 2: LogConfig config);
  LogConfig getLogConfig(1: string name);

  /**
  * Enable/disable logging the tuples generated in topology via an internal EventLogger bolt. The component name is optional
  * and if null or empty, the debug flag will apply to the entire topology.
  *
  * The 'samplingPercentage' will limit loggging to a percentage of generated tuples.
  **/
  void debug(1: string name, 2: string component, 3: bool enable, 4: double samplingPercentage) throws (1: NotAliveException e, 2: AuthorizationException aze);

  // dynamic profile actions
  void setWorkerProfiler(1: string id, 2: ProfileRequest  profileRequest);
  list<ProfileRequest> getComponentPendingProfileActions(1: string id, 2: string component_id, 3: ProfileAction action);

  void uploadNewCredentials(1: string name, 2: Credentials creds) throws (1: NotAliveException e, 2: InvalidTopologyException ite, 3: AuthorizationException aze);

  // need to add functions for asking about status of storms, what nodes they're running on, looking at task logs

  string beginFileUpload() throws (1: AuthorizationException aze);
  void uploadChunk(1: string location, 2: binary chunk) throws (1: AuthorizationException aze);
  void finishFileUpload(1: string location) throws (1: AuthorizationException aze);
  
  string beginFileDownload(1: string file) throws (1: AuthorizationException aze);
  //can stop downloading chunks when receive 0-length byte array back
  binary downloadChunk(1: string id) throws (1: AuthorizationException aze);

  // returns json
  string getNimbusConf() throws (1: AuthorizationException aze);
  // stats functions
  ClusterSummary getClusterInfo() throws (1: AuthorizationException aze);
  TopologyInfo getTopologyInfo(1: string id) throws (1: NotAliveException e, 2: AuthorizationException aze);
  TopologyInfo getTopologyInfoWithOpts(1: string id, 2: GetInfoOptions options) throws (1: NotAliveException e, 2: AuthorizationException aze);
  TopologyPageInfo getTopologyPageInfo(1: string id, 2: string window, 3: bool is_include_sys) throws (1: NotAliveException e, 2: AuthorizationException aze);
  ComponentPageInfo getComponentPageInfo(1: string topology_id, 2: string component_id, 3: string window, 4: bool is_include_sys) throws (1: NotAliveException e, 2: AuthorizationException aze);
  //returns json
  string getTopologyConf(1: string id) throws (1: NotAliveException e, 2: AuthorizationException aze);
  /**
   * Returns the compiled topology that contains ackers and metrics consumsers. Compare {@link #getUserTopology(String id)}.
   */
  StormTopology getTopology(1: string id) throws (1: NotAliveException e, 2: AuthorizationException aze);
  /**
   * Returns the user specified topology as submitted originally. Compare {@link #getTopology(String id)}.
   */
  StormTopology getUserTopology(1: string id) throws (1: NotAliveException e, 2: AuthorizationException aze);
  TopologyHistoryInfo getTopologyHistory(1: string user) throws (1: AuthorizationException aze);
}

struct DRPCRequest {
  1: required string func_args;
  2: required string request_id;
}

exception DRPCExecutionException {
  1: required string msg;
}

service DistributedRPC {
  string execute(1: string functionName, 2: string funcArgs) throws (1: DRPCExecutionException e, 2: AuthorizationException aze);
}

service DistributedRPCInvocations {
  void result(1: string id, 2: string result) throws (1: AuthorizationException aze);
  DRPCRequest fetchRequest(1: string functionName) throws (1: AuthorizationException aze);
  void failRequest(1: string id) throws (1: AuthorizationException aze);  
}
