/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;


/**
 * A class that maintains the broker and a sorted set of replicas based on a given comparator.
 */
public class BrokerAndSortedReplicas {
  private final Broker _broker;
  private final Map<Replica, ReplicaWrapper> _replicaWrapperMap;
  private final NavigableSet<ReplicaWrapper> _sortedReplicas;
  private final Function<Replica, Double> _scoreFunction;

  public BrokerAndSortedReplicas(Broker broker, Function<Replica, Double> scoreFunction) {
    _broker = broker;
    _sortedReplicas = new TreeSet<>();
    _replicaWrapperMap = new HashMap<>();
    _scoreFunction = scoreFunction;
    broker.replicas().forEach(this::add);
  }

  public Broker broker() {
    return _broker;
  }

  public NavigableSet<ReplicaWrapper> sortedReplicas() {
    return Collections.unmodifiableNavigableSet(_sortedReplicas);
  }

  public void add(ReplicaWrapper rw) {
    ReplicaWrapper old = _replicaWrapperMap.put(rw.replica(), rw);
    if (old != null) {
      _sortedReplicas.remove(old);
    }
    _sortedReplicas.add(rw);
  }

  public void add(Replica replica) {
    double score = _scoreFunction.apply(replica);
    ReplicaWrapper rw = new ReplicaWrapper(replica, score);
    add(rw);
  }

  public ReplicaWrapper remove(Replica replica) {
    ReplicaWrapper rw = _replicaWrapperMap.remove(replica);
    _sortedReplicas.remove(rw);
    return rw;
  }

  ReplicaWrapper get(Replica replica) {
    return _replicaWrapperMap.get(replica);
  }

  Map<Replica, ReplicaWrapper> replicaWrapperMap() {
    return _replicaWrapperMap;
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
