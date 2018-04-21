/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.estimator.impl;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.linkedin.cruisecontrol.CruiseControlUtils;
import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.model.estimator.MetricEstimator;
import com.linkedin.cruisecontrol.model.regression.LinearRegression;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


/**
 * An abstract class that implements the {@link MetricEstimator} using {@link LinearRegression}.
 * <p>
 *   This class reads the causal relations from a config file in the following format:
 * </p>
 * <pre>
 *   {
 *     linearRegressions: [
 *       {
 *         "name": "regression_1",
 *         "causalMetrics": ["CausalMetric_1", "CausalMetric_2", "CausalMetric_3"],
 *         "resultantMetrics": ["ResultantMetric_1", "ResultantMetric_2"],
 *         "numValueBuckets": 20,
 *         "numBucketsToCapForFairness": 10,
 *         "maxSamplesPerBucket": 50,
 *         "maxExponent": 2,
 *         "errorStatsWindowSize": 1000
 *       },
 *       {
 *         "name": "regression_2",
 *         "causalMetrics": ["CausalMetric_1", "CausalMetric_2"],
 *         "resultantMetrics": ["ResultantMetric_3", "ResultantMetric_4"],
 *         "numValueBuckets": 20,
 *         "numBucketsToCapForFairness": 10,
 *         "maxSamplesPerBucket": 50,
 *         "maxExponent": 2,
 *         "errorStatsWindowSize": 1000
 *       },
 *       ...
 *     ]
 *   }
 * </pre>
 * The each regression may contain multiple resultant metrics if they share the same causal metrics. These resultant
 * metrics are, however, independent to each other.
 *
 * This estimator assumes all the metric samples share the same linear regression model, i.e. this estimator assumes
 * homogeneous entities. The {@link #estimate(Entity, MetricInfo, AggregatedMetricValues)} call ignores the passed
 * in entities. Use {@link HeterogeneousLinearRegressionEstimator} for heterogeneous use cases.
 */
public abstract class LinearRegressionEstimator implements MetricEstimator {
  public static final String CAUSAL_RELATION_DEFINITION_FILE_CONFIG = "causal.relation.definition.file";
  private final Map<String, LinearRegression> _linearRegressionMap;
  private final Map<MetricInfo, SortedSet<LinearRegression>> _linearRegressionByResultantMetrics;

  public LinearRegressionEstimator() {
    _linearRegressionMap = new HashMap<>();
    _linearRegressionByResultantMetrics = new HashMap<>();
  }

  /**
   * @return the metric definition to use for linear regression.
   */
  protected abstract MetricDef metricDef();

  @Override
  public void addTrainingSamples(Collection<MetricSample> trainingSamples) {
    _linearRegressionMap.values().forEach(lr -> lr.addMetricSamples(trainingSamples));
    updateLinearRegressionRank();
  }

  @Override
  public float trainingProgress() {
    double progress = 0.0f;
    for (LinearRegression lr : _linearRegressionMap.values()) {
      progress = Math.max(progress, lr.modelCoefficientTrainingCompleteness());
    }
    return (float) progress / _linearRegressionMap.size();
  }

  @Override
  public String trainingProgressDescription() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, LinearRegression> entry : _linearRegressionMap.entrySet()) {
      String name = entry.getKey();
      LinearRegression lr = entry.getValue();
      sb.append(name).append(": \n").append(lr.modelState()).append("\n\n");
    }
    return sb.toString();
  }

  @Override
  public void train() {
    _linearRegressionMap.values().forEach(LinearRegression::updateModelCoefficient);
  }

  @Override
  public MetricValues estimate(Entity entity, MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    LinearRegression bestRegression = bestLinearRegression(resultantMetric);
    return bestRegression.estimate(resultantMetric, causalMetricValues, null, null);
  }

  @Override
  public MetricValues estimate(Entity entity,
                               MetricInfo resultantMetric,
                               AggregatedMetricValues causalMetricValues,
                               AggregatedMetricValues causalMetricValueChanges,
                               ChangeType changeType) {
    LinearRegression bestRegression = bestLinearRegression(resultantMetric);
    return bestRegression.estimate(resultantMetric, causalMetricValues, causalMetricValueChanges, changeType);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    String linearRegressionDefinitionFile = (String) configs.get(CAUSAL_RELATION_DEFINITION_FILE_CONFIG);
    CruiseControlUtils.ensureValidString(CAUSAL_RELATION_DEFINITION_FILE_CONFIG, linearRegressionDefinitionFile);
    try {
      loadLinearRegressions(linearRegressionDefinitionFile);
    } catch (FileNotFoundException e) {
      throw new ConfigException("Linear regression definition file " + linearRegressionDefinitionFile
                                    + " is not found.", e);
    }
  }
  
  // Methods specific to the linear regression.
  /**
   * From the best linear regression, get the coefficients of the given causal metric for the given resultant metric 
   * with all the exponents.
   *
   * @param resultantMetric the resultant metric to get the coefficients for.
   * @param causalMetric the causal metric whose coefficients should be returned.
   * @return A mapping from exponents to the coefficient of the given causal metric and resultant metric combination.
   */
  public Map<Integer, Double> getCoefficients(MetricInfo resultantMetric, MetricInfo causalMetric) {
    return bestLinearRegression(resultantMetric).getCoefficients(resultantMetric, causalMetric);
  }

  /**
   * Estimate the average value of the given resultant metric using the average values of the given causal metrics.
   *
   * @param resultantMetric the resultant metric whose value is to be estimated.
   * @param causalMetricValues the causal metrics values.
   * @return the estimated average value of the given resultant metric.
   */
  public double estimateAvg(MetricInfo resultantMetric, AggregatedMetricValues causalMetricValues) {
    return bestLinearRegression(resultantMetric).estimateAvg(resultantMetric, causalMetricValues);
  }

  /**
   * Get the estimation error statistics of the best linear regression for the given resultant metric.
   * 
   * @param resultantMetric the resultant metric to get the estimation error statistics.
   * @return the estimation error statistics of this linear regression for the given resultant metric
   */
  public DescriptiveStatistics estimationErrorStats(MetricInfo resultantMetric) {
    return bestLinearRegression(resultantMetric).estimationErrorStats(resultantMetric);
  }

  /**
   * Get the state of this best linear regression for the given resultant metric.
   * 
   * @return the state of this linear regression.
   */
  public LinearRegression.LinearRegressionState modelState(MetricInfo resultantMetric) {
    return bestLinearRegression(resultantMetric).modelState();
  }
  
  // private helper methods.
  /**
   * Get the best linear regression for the resultant metric. 
   * The best linear regression is defined using {@link LinearRegressionComparator}.
   * @param resultantMetric the resultant metric to get the the best linear regression. 
   * @return the best linear regression for the given resultant metric.
   */
  private LinearRegression bestLinearRegression(MetricInfo resultantMetric) {
    LinearRegression bestRegression = _linearRegressionByResultantMetrics.get(resultantMetric).first();
    if (bestRegression == null || !bestRegression.trainingCompleted(resultantMetric)) {
      throw new IllegalStateException("The linear regression for resultant metric " + resultantMetric
                                          + " is not ready.");
    }
    return bestRegression;
  }
  
  private void loadLinearRegressions(String linearRegressionDefinitionFile) throws FileNotFoundException {
    JsonReader reader = null;
    try {
      reader = new JsonReader(new InputStreamReader(new FileInputStream(linearRegressionDefinitionFile), StandardCharsets.UTF_8));
      Gson gson = new Gson();
      List<LinearRegressionDef> linearRegressionDefList =
          ((LinearRegressionList) gson.fromJson(reader, LinearRegressionList.class)).linearRegressions;
      for (LinearRegressionDef linearRegressionDef : linearRegressionDefList) {
        Set<MetricInfo> resultantMetrics = toMetricInfoSet(linearRegressionDef.resultantMetrics);
        LinearRegression lr = new LinearRegression(linearRegressionDef.name,
                                                   resultantMetrics,
                                                   toMetricInfoSet(linearRegressionDef.causalMetrics),
                                                   linearRegressionDef.numValueBuckets,
                                                   linearRegressionDef.numBucketsToCapForFairness,
                                                   linearRegressionDef.maxSamplesPerBucket,
                                                   linearRegressionDef.maxExponent,
                                                   linearRegressionDef.errorStatsWindowSize);
        if (_linearRegressionMap.put(linearRegressionDef.name, lr) != null) {
          throw new ConfigException("Linear regression " + linearRegressionDef.name + " is defined more than once.");
        }
        for (MetricInfo resultantMetric : resultantMetrics) {
          _linearRegressionByResultantMetrics.computeIfAbsent(resultantMetric,
                                                              rm -> new TreeSet<>(new LinearRegressionComparator(resultantMetric)))
                                             .add(lr);
        }
      }
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        // let it go.
      }
    }
  }

  private void updateLinearRegressionRank() {
    for (SortedSet<LinearRegression> linearRegressions : _linearRegressionByResultantMetrics.values()) {
      Set<LinearRegression> temp = new HashSet<>(linearRegressions);
      linearRegressions.clear();
      linearRegressions.addAll(temp);
    }
  }

  private Set<MetricInfo> toMetricInfoSet(Set<String> metricNames) {
    Set<MetricInfo> metricInfoSet = new HashSet<>();
    for (String metricName : metricNames) {
      metricInfoSet.add(metricDef().metricInfo(metricName));
    }
    return metricInfoSet;
  }

  /**
   * A class that compares multiple linear regressions for the same resultant metric. The smaller the error
   * is the higher the linear regression is ranked.
   */
  private static class LinearRegressionComparator implements Comparator<LinearRegression> {
    private final MetricInfo _resultantMetric;

    LinearRegressionComparator(MetricInfo resultantMetric) {
      _resultantMetric = resultantMetric;
    }

    @Override
    public int compare(LinearRegression lr1, LinearRegression lr2) {
      // First check if the estimation has finished or not.
      if (!lr2.trainingCompleted(_resultantMetric)) {
        return -1;
      } else if (!lr1.trainingCompleted(_resultantMetric)) {
        return 1;
      }
      // If both are finished, check the error percentile.
      DescriptiveStatistics errStats1 = lr1.estimationErrorStats(_resultantMetric);
      DescriptiveStatistics errStats2 = lr2.estimationErrorStats(_resultantMetric);
      // Compare the errors in this percentile order.
      for (double percentile : Arrays.asList(95.0, 90.0, 75.0, 50.0)) {
        int result = Double.compare(errStats1.getPercentile(percentile), errStats2.getPercentile(percentile));
        if (result != 0) {
          return result;
        }
      }
      // Lastly check the name.
      return lr1.name().compareTo(lr2.name());
    }
  }

  /**
   * Classes for JSON parsing using GSON.
   */
  private class LinearRegressionList {
    List<LinearRegressionDef> linearRegressions;
  }

  private class LinearRegressionDef {
    String name;
    Set<String> causalMetrics;
    Set<String> resultantMetrics;
    int numValueBuckets;
    int numBucketsToCapForFairness;
    int maxSamplesPerBucket;
    int maxExponent;
    int errorStatsWindowSize;
  }
}
