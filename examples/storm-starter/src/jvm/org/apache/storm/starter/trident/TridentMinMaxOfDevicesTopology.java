/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.spout.RandomNumberGeneratorSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class demonstrates different usages of
 * * {@link Stream#minBy(String)}
 * * {@link Stream#maxBy(String)}
 * operations on trident {@link Stream}.
 */
public class TridentMinMaxOfDevicesTopology {

    /**
     * Creates a topology with device-id and count (which are whole numbers) as tuple fields in a stream and it finally
     * generates result stream based on min amd max with device-id and count values.
     */
    public static StormTopology buildDevicesTopology() {
        String deviceID = "device-id";
        String count = "count";
        Fields allFields = new Fields(deviceID, count);

        RandomNumberGeneratorSpout spout = new RandomNumberGeneratorSpout(allFields, 10, 1000);

        TridentTopology topology = new TridentTopology();
        Stream devicesStream = topology.newStream("devicegen-spout", spout).
                each(allFields, new Debug("##### devices"));

        devicesStream.minBy(deviceID).
                each(allFields, new Debug("#### device with min id"));

        devicesStream.maxBy(count).
                each(allFields, new Debug("#### device with max count"));

        return topology.build();
    }

    /**
     * Creates a topology which demonstrates min/max operations on tuples of stream which contain vehicle and driver fields
     * with values {@link TridentMinMaxOfDevicesTopology.Vehicle} and {@link TridentMinMaxOfDevicesTopology.Driver} respectively.
     */
    public static StormTopology buildVehiclesTopology() {
        Fields driverField = new Fields(Driver.FIELD_NAME);
        Fields vehicleField = new Fields(Vehicle.FIELD_NAME);
        Fields allFields = new Fields(Vehicle.FIELD_NAME, Driver.FIELD_NAME);

        FixedBatchSpout spout = new FixedBatchSpout(allFields, 10, Vehicle.generateVehicles(20));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream vehiclesStream = topology.newStream("spout1", spout).
                each(allFields, new Debug("##### vehicles"));

        Stream slowVehiclesStream =
                vehiclesStream
                        .min(new SpeedComparator())
                        .each(vehicleField, new Debug("#### slowest vehicle"));

        Stream slowDriversStream =
                slowVehiclesStream
                        .project(driverField)
                        .each(driverField, new Debug("##### slowest driver"));

        vehiclesStream
                .max(new SpeedComparator())
                .each(vehicleField, new Debug("#### fastest vehicle"))
                .project(driverField)
                .each(driverField, new Debug("##### fastest driver"));

        vehiclesStream
                .max(new EfficiencyComparator()).
                each(vehicleField, new Debug("#### efficient vehicle"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        StormTopology topology = buildDevicesTopology();
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("devices-topology", conf, topology);
            Utils.sleep(60 * 1000);
            cluster.shutdown();
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar("devices-topology", conf, topology);
        }
    }

    static class SpeedComparator implements Comparator<TridentTuple>, Serializable {

        @Override
        public int compare(TridentTuple tuple1, TridentTuple tuple2) {
            Vehicle vehicle1 = (Vehicle) tuple1.getValueByField(Vehicle.FIELD_NAME);
            Vehicle vehicle2 = (Vehicle) tuple2.getValueByField(Vehicle.FIELD_NAME);
            return Integer.compare(vehicle1.maxSpeed, vehicle2.maxSpeed);
        }
    }

    static class EfficiencyComparator implements Comparator<TridentTuple>, Serializable {

        @Override
        public int compare(TridentTuple tuple1, TridentTuple tuple2) {
            Vehicle vehicle1 = (Vehicle) tuple1.getValueByField(Vehicle.FIELD_NAME);
            Vehicle vehicle2 = (Vehicle) tuple2.getValueByField(Vehicle.FIELD_NAME);
            return Double.compare(vehicle1.efficiency, vehicle2.efficiency);
        }

    }

    static class Driver implements Serializable {
        static final String FIELD_NAME = "driver";
        final String name;
        final int id;

        Driver(String name, int id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public String toString() {
            return "Driver{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }

    static class Vehicle implements Serializable {
        static final String FIELD_NAME = "vehicle";
        final String name;
        final int maxSpeed;
        final double efficiency;

        public Vehicle(String name, int maxSpeed, double efficiency) {
            this.name = name;
            this.maxSpeed = maxSpeed;
            this.efficiency = efficiency;
        }

        @Override
        public String toString() {
            return "Vehicle{" +
                    "name='" + name + '\'' +
                    ", maxSpeed=" + maxSpeed +
                    ", efficiency=" + efficiency +
                    '}';
        }

        public static List<Object>[] generateVehicles(int count) {
            List<Object>[] vehicles = new List[count];
            for (int i = 0; i < count; i++) {
                int id = i - 1;
                vehicles[i] =
                        (new Values(
                                new Vehicle("Vehicle-" + id, ThreadLocalRandom.current().nextInt(0, 100), ThreadLocalRandom.current().nextDouble(1, 5)),
                                new Driver("Driver-" + id, id)
                        ));
            }
            return vehicles;
        }
    }
}
