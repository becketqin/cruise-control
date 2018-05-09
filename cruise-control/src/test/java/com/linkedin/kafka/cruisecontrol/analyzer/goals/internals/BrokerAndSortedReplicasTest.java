/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class BrokerAndSortedReplicasTest {
  private static final TopicPartition TP0 = new TopicPartition("Test", 0);
  private static final TopicPartition TP1 = new TopicPartition("Test", 1);
  private static final TopicPartition TP2 = new TopicPartition("Test", 2);
  private static final TopicPartition TP3 = new TopicPartition("Test", 3);

  @Test
  public void testAddAndRemove() {
    ClusterModel clusterModel = getClusterModel();
    BrokerAndSortedReplicas bas = new BrokerAndSortedReplicas(clusterModel.broker(0), this::metricValue);
    Replica replica = clusterModel.broker(1).replica(TP2);
    bas.add(replica);
    assertNotNull(bas.remove(replica));
    assertOrder(bas.sortedReplicas());
    assertEquals(2, bas.replicaWrapperMap().size());
    assertEquals(2, bas.sortedReplicas().size());
  }

  @Test
  public void testAddAndRemoveAfterLoadChange() {
    ClusterModel clusterModel = getClusterModel();
    BrokerAndSortedReplicas bas = new BrokerAndSortedReplicas(clusterModel.broker(0), this::metricValue);
    Replica replica = clusterModel.broker(1).replica(TP2);
    bas.add(replica);
    replica.load().loadByWindows().valuesFor(0).set(0, 100);
    assertNotNull(bas.remove(clusterModel.broker(1).replica(TP2)));
    assertOrder(bas.sortedReplicas());
    assertEquals(2, bas.replicaWrapperMap().size());
    assertEquals(2, bas.sortedReplicas().size());
  }

  @Test
  public void testAddReplicaWrapper() {
    ClusterModel clusterModel = getClusterModel();
    BrokerAndSortedReplicas bas = new BrokerAndSortedReplicas(clusterModel.broker(0), this::metricValue);
    Replica replica = clusterModel.broker(1).replica(TP2);

    bas.add(replica);
    assertEquals(30, bas.get(replica).score(), 0.0);
    assertEquals(3, bas.sortedReplicas().size());
    assertEquals(3, bas.replicaWrapperMap().size());
    assertOrder(bas.sortedReplicas());

    bas.add(new ReplicaWrapper(replica, 5));
    assertEquals(5, bas.get(replica).score(), 0.0);
    assertEquals(3, bas.sortedReplicas().size());
    assertEquals(3, bas.replicaWrapperMap().size());
    assertOrder(bas.sortedReplicas());

    assertEquals(5, bas.remove(clusterModel.broker(1).replica(TP2)).score(), 0.0);

    assertEquals(2, bas.replicaWrapperMap().size());
    assertEquals(2, bas.sortedReplicas().size());
  }

  private void assertOrder(NavigableSet<ReplicaWrapper> replicaWrappers) {
    double lastValue = Double.NEGATIVE_INFINITY;
    for (ReplicaWrapper replicaWrapper : replicaWrappers) {
      assertTrue(replicaWrapper.score() >= lastValue);
      lastValue = replicaWrapper.score();
    }
  }

  private double metricValue(Replica replica) {
    return replica.load().loadByWindows().valuesFor(0).get(0);
  }

  private ClusterModel getClusterModel() {

    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L),
                                                 1.0);
    clusterModel.createRack("rack");
    clusterModel.createBroker("rack", "host0", 0, getBrokerCapacities());
    clusterModel.createBroker("rack", "host1", 1, getBrokerCapacities());
    clusterModel.createReplica("rack", 0, TP0, 0, true);
    clusterModel.createReplica("rack", 0, TP1, 0, true);
    clusterModel.createReplica("rack", 1, TP2, 0, true);
    clusterModel.createReplica("rack", 1, TP3, 0, true);
    clusterModel.setReplicaLoad("rack", 0, TP0, getAggMetricValues(10), Collections.singletonList(0L));
    clusterModel.setReplicaLoad("rack", 0, TP1, getAggMetricValues(20), Collections.singletonList(0L));
    clusterModel.setReplicaLoad("rack", 1, TP2, getAggMetricValues(30), Collections.singletonList(0L));
    clusterModel.setReplicaLoad("rack", 1, TP3, getAggMetricValues(40), Collections.singletonList(0L));
    return clusterModel;
  }

  private Map<Resource, Double> getBrokerCapacities() {
    Map<Resource, Double> capacities = new HashMap<>();
    Resource.cachedValues().forEach(r -> capacities.put(r, 100.0));
    return capacities;
  }

  private AggregatedMetricValues getAggMetricValues(double metricValue) {
    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues();
    MetricValues metricValues = new MetricValues(1);
    metricValues.set(0, metricValue);
    aggregatedMetricValues.add(0, metricValues);
    return aggregatedMetricValues;
  }
}
