/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;

import java.util.Collection;
import java.util.NavigableSet;


/**
 * The replica finder provides necessary information needed to search for a replica in a replica set.
 * While the interface is agnostic to the searching algorithm, a given implementation may only be
 * applicable to a certain searching algorithm.
 */
public interface ReplicaFinder {

  /**
   * Get a navigable set of {@link ReplicaWrapper} that this replica finder is going to search upon. The returned
   * set should be sorted in ascending order.
   *
   * @return a navigable set of {@link ReplicaWrapper} to perform search on.
   */
  NavigableSet<ReplicaWrapper> replicasToSearch();

  /**
   * Check if the given replica is acceptable. The return result is important to the efficiency of the searching
   * algorithm. The implementation should have sufficient context to decide which
   * {@link com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaSearchResult.ReplicaCheckResult} should
   * be returned.
   *
   * @param replica the replica to check acceptance for.
   * @param replicaSearchResult the replica search progress to update.
   *
   * @return The {@link ActionAcceptance} of the balancing action.
   */
  void checkReplica(Replica replica, ReplicaSearchResult replicaSearchResult);

  /**
   * Evaluate the replica to see if the given replica meets the requirement. This is used by
   * {@link ReplicaSearchAlgorithm#binarySearchForReplica(ReplicaFinder)}
   *
   * @param replica the replica to evaluate.
   * @return positive if a larger replica is needed, negative if a smaller replica is needed, or 0 if the replica
   *         is a perfect match.
   */
  int evaluate(Replica replica);

  /**
   * Choose a replica between two candidate replicas. This is used by
   * {@link ReplicaSearchAlgorithm#searchForClosestLegitReplica(ReplicaFinder, ReplicaWrapper)}
   *
   * @param r1 the first candidate replica.
   * @param r2 the second candidate replica.
   *
   * @return the replica that is chosen.
   */
  ReplicaWrapper choose(ReplicaWrapper r1, ReplicaWrapper r2);

}
