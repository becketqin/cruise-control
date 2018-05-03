/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;


/**
 * A class that maintains the broker and a sorted set of replicas based on a given comparator.
 */
public class BrokerAndSortedReplicas {
  private final Broker _broker;
  private final NavigableSet<ReplicaWrapper> _sortedReplicas;

  public BrokerAndSortedReplicas(Broker broker, Function<Replica, Double> scoreFunction) {
    _broker = broker;
    _sortedReplicas = new TreeSet<>();
    broker.replicas().forEach(r -> {
      double score = scoreFunction.apply(r);
      _sortedReplicas.add(new ReplicaWrapper(r, score));
    });
  }

  public Broker broker() {
    return _broker;
  }

  public NavigableSet<ReplicaWrapper> sortedReplicas() {
    return _sortedReplicas;
  }

  @Override
  public int hashCode() {
    return _broker.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof BrokerAndSortedReplicas
        && _broker.equals(((BrokerAndSortedReplicas) obj).broker());
  }
}
