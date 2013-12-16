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
}

struct SupervisorSummary {
  1: required string host;
  2: required i32 uptime_secs;
  3: required i32 num_workers;
  4: required i32 num_used_workers;
  5: required string supervisor_id;
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
}

struct KillOptions {
  1: optional i32 wait_secs;
}

struct RebalanceOptions {
  1: optional i32 wait_secs;
  2: optional i32 num_workers;
  3: optional map<string, i32> num_executors;
}

enum TopologyInitialStatus {
    ACTIVE = 1,
    INACTIVE = 2
}
struct SubmitOptions {
  1: required TopologyInitialStatus initial_status;
}

service Nimbus {
  void submitTopology(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
  void submitTopologyWithOpts(1: string name, 2: string uploadedJarLocation, 3: string jsonConf, 4: StormTopology topology, 5: SubmitOptions options) throws (1: AlreadyAliveException e, 2: InvalidTopologyException ite);
  void killTopology(1: string name) throws (1: NotAliveException e);
  void killTopologyWithOpts(1: string name, 2: KillOptions options) throws (1: NotAliveException e);
  void activate(1: string name) throws (1: NotAliveException e);
  void deactivate(1: string name) throws (1: NotAliveException e);
  void rebalance(1: string name, 2: RebalanceOptions options) throws (1: NotAliveException e, 2: InvalidTopologyException ite);

  // need to add functions for asking about status of storms, what nodes they're running on, looking at task logs

  string beginFileUpload();
  void uploadChunk(1: string location, 2: binary chunk);
  void finishFileUpload(1: string location);
  
  string beginFileDownload(1: string file);
  //can stop downloading chunks when receive 0-length byte array back
  binary downloadChunk(1: string id);

  // returns json
  string getNimbusConf();
  // stats functions
  ClusterSummary getClusterInfo();
  TopologyInfo getTopologyInfo(1: string id) throws (1: NotAliveException e);
  //returns json
  string getTopologyConf(1: string id) throws (1: NotAliveException e);
  StormTopology getTopology(1: string id) throws (1: NotAliveException e);
  StormTopology getUserTopology(1: string id) throws (1: NotAliveException e);
}

struct DRPCRequest {
  1: required string func_args;
  2: required string request_id;
}

exception DRPCExecutionException {
  1: required string msg;
}

service DistributedRPC {
  string execute(1: string functionName, 2: string funcArgs) throws (1: DRPCExecutionException e);
}

service DistributedRPCInvocations {
  void result(1: string id, 2: string result);
  DRPCRequest fetchRequest(1: string functionName);
  void failRequest(1: string id);  
}
