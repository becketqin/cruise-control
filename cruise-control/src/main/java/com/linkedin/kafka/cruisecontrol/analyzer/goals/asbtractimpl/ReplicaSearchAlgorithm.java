/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.ReplicaWrapper;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.*;


/**
 * A util class containing the replica searching algorithm on top of {@link ReplicaFinder}.
 */
public class ReplicaSearchAlgorithm {

  private ReplicaSearchAlgorithm() {

  }

  /**
   * Find a replica using the replica finder.
   * @param replicaFinder the replica finder.
   * @return a fitting replica that meets the requirements of the replica finder, or null if no such replica is found.
   */
  public static ReplicaWrapper searchForReplica(ReplicaFinder replicaFinder) {
    NavigableSet<ReplicaWrapper> sortedCandidateReplicas = replicaFinder.replicasToSearch();
    if (sortedCandidateReplicas == null || sortedCandidateReplicas.isEmpty()) {
      return null;
    }

    ReplicaWrapper candidate = binarySearchForReplica(replicaFinder);
    if (candidate != null) {
      candidate = searchForClosestLegitReplica(replicaFinder, candidate);
    }
    return candidate;
  }

  /**
   * Search in the given sortedReplicasToSwapWith to find a replica that shrinks the metric value diff between
   * two brokers most (the broker owning replicaToSwap and the broker owns the sortedReplicasToSwapWith), while still
   * meets all the specified requirements.
   *
   * Note that this method is based on the assumption that the ranking of impact score of the replicas are the same
   * for different brokers, i.e. if replica <tt>R1</tt> has a higher impact score of a metric value than replica
   * <tt>R2</tt> in broker <tt>B</tt>, <tt>R1</tt> also has a higher impact score of that metric value than
   * <tt>R2</tt> in any other brokers.
   *
   * While the above requirement is true for any directly accumulative metrics, it may not always be true in cases
   * for some of th metrics when causal relation is used. In that case, instead of using binary search, a linear
   * traversal is preferred at the cost of slower calculation.
   *
   * @param replicaFinder the balancing action finder.
   *
   * @return A candidate replica that shrinks the gap between two brokers and meets all the requirements after the
   *         action, or null if no such candidate replica was found.
   */
  public static ReplicaWrapper binarySearchForReplica(ReplicaFinder replicaFinder) {
    NavigableSet<ReplicaWrapper> sortedCandidateReplicas = replicaFinder.replicasToSearch();
    if (sortedCandidateReplicas.isEmpty()) {
      return null;
    }

    if (replicaFinder.evaluate(sortedCandidateReplicas.last().replica()) > 0) {
      return sortedCandidateReplicas.last();
    } else if (replicaFinder.evaluate(sortedCandidateReplicas.first().replica()) < 0) {
      return sortedCandidateReplicas.first();
    }

    // Do a binary search to find the best candidate replica.
    // The candidate could be chosen if :
    // 1. it makes the metric value of the broker toSwap getting as closer to its target metric value, AND
    // 2. the swap does not make the metric value of the broker toSwapWith become worse than the value of the broker
    //    toSwap before the swap.
    NavigableSet<ReplicaWrapper> candidateReplicas = sortedCandidateReplicas;
    ReplicaWrapper candidate = null;
    do {
      double lowScore = candidateReplicas.first().score();
      double highScore = candidateReplicas.last().score();
      double midScore = (lowScore + highScore) / 2;
      // The candidate will not be null here.
      candidate = candidateReplicas.ceiling(ReplicaWrapper.lessThanOrEqualsTo(midScore));

      // Get the next candidate replica set,
      double evaluation = replicaFinder.evaluate(candidate.replica());
      if (evaluation > 0) {
        candidateReplicas = candidateReplicas.tailSet(candidate, false);
      } else if (evaluation < 0) {
        candidateReplicas = candidateReplicas.headSet(candidate, false);
      } else {
        break;
      }
    } while (!candidateReplicas.isEmpty());
    return candidate;
  }

  public static ReplicaWrapper searchForClosestLegitReplica(ReplicaFinder replicaFinder,
                                                            ReplicaWrapper startingReplica) {
    NavigableSet<ReplicaWrapper> sortedCandidateReplicas = replicaFinder.replicasToSearch();
    if (sortedCandidateReplicas.isEmpty()) {
      return null;
    }
    // Check if the ideal replica is available and acceptable.
    if (sortedCandidateReplicas.contains(startingReplica)) {
      ActionAcceptance startingReplicaAcceptance = replicaFinder.isReplicaAcceptable(startingReplica.replica());
      if (startingReplicaAcceptance == ACCEPT) {
        return startingReplica;
      } else if (startingReplicaAcceptance == BROKER_REJECT) {
        return null;
      }
    }

    // Prepare for the search. We need to handle the case that he ideal replica is not in the range of the provided
    // sorted replica set.
    NavigableSet<ReplicaWrapper> smallerReplicas;
    NavigableSet<ReplicaWrapper> largerReplicas;
    if (startingReplica.compareTo(sortedCandidateReplicas.first()) < 0) {
      smallerReplicas = Collections.emptyNavigableSet();
      largerReplicas = sortedCandidateReplicas;
    } else if (startingReplica.compareTo(sortedCandidateReplicas.last()) > 0) {
      smallerReplicas = sortedCandidateReplicas;
      largerReplicas = Collections.emptyNavigableSet();
    } else {
      smallerReplicas = sortedCandidateReplicas.headSet(startingReplica, false);
      largerReplicas = sortedCandidateReplicas.tailSet(startingReplica, false);
    }

    // Perform the search starting at the ideal replica. The search goes towards two directions, and stop once
    // we find a acceptable replica.
    Iterator<ReplicaWrapper> descendingIter = smallerReplicas.descendingIterator();
    Iterator<ReplicaWrapper> ascendingIter = largerReplicas.iterator();
    while (descendingIter.hasNext() || ascendingIter.hasNext()) {
      ReplicaWrapper smallerReplica = nextLegitReplica(descendingIter, replicaFinder);
      ReplicaWrapper largerReplica = nextLegitReplica(ascendingIter, replicaFinder);
      ReplicaWrapper winner = replicaFinder.choose(smallerReplica, largerReplica);
      if (winner != null) {
        return winner;
      }
    }
    return null;
  }

  private static ReplicaWrapper nextLegitReplica(Iterator<ReplicaWrapper> iter, ReplicaFinder replicaFinder) {
    ReplicaWrapper replicaWrapper = null;
    while (iter.hasNext() && replicaWrapper == null) {
      replicaWrapper = iter.next();
      ActionAcceptance acceptance = replicaFinder.isReplicaAcceptable(replicaWrapper.replica());
      // We only need to check replica reject, as broker reject is already checked on the starting replica.
      if (acceptance == REPLICA_REJECT) {
        replicaWrapper = null;
      }
    }
    return replicaWrapper;
  }
}
