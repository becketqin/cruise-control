/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.regression;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC1;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC2;
import static com.linkedin.cruisecontrol.CruiseControlUnitTestUtils.METRIC3;
import static org.junit.Assert.*;


public class RegressionMetricsAccumulatorTest {
  private CausalRelation _causalRelation;
  private MetricDef _metricDef;
  
  @Before
  public void prepare() {
    _metricDef = CruiseControlUnitTestUtils.getMetricDef();
    _causalRelation = new CausalRelation(Collections.singleton(_metricDef.metricInfo(METRIC1)), 
                                         Arrays.asList(_metricDef.metricInfo(METRIC2), _metricDef.metricInfo(METRIC3)));
  }
  
  @Test
  public void testRecordMetricValues() {
    List<double[]> data = prepareData(10);
    RegressionMetricsAccumulator accumulator = new RegressionMetricsAccumulator(_causalRelation, 5, 3, 5, 1);
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    // Should not have removed anything.
    Map<MetricInfo, MetricDiversity> diversityForMetrics = accumulator.diversityForMetrics();
    accumulator.diversityForMetrics();
    assertEquals(_causalRelation.indexes().size(), diversityForMetrics.size());
    for (MetricDiversity diversity : diversityForMetrics.values()) {
      Map<Integer, Integer> counts = diversity.countsByBucket();
      for (int i = 0; i < 5; i++) {
        assertEquals("There should be 2 values in bucket " + i,2, counts.get(i).intValue());
      }
    }
  }

  @Test
  public void testMaybeRemoveOldValues() {
    List<double[]> data = prepareData(100);
    RegressionMetricsAccumulator accumulator = new RegressionMetricsAccumulator(_causalRelation, 5, 3, 5, 1);
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    // Should have removed additional value samples.
    Map<MetricInfo, MetricDiversity> diversityForMetrics = accumulator.diversityForMetrics();
    accumulator.diversityForMetrics();
    assertEquals(_causalRelation.indexes().size(), diversityForMetrics.size());
    for (MetricDiversity diversity : diversityForMetrics.values()) {
      Map<Integer, Integer> counts = diversity.countsByBucket();
      for (int i = 0; i < 5; i++) {
        assertEquals("There should be 2 values in bucket " + i,5, counts.get(i).intValue());
      }
    }
    // Check the samples
    Map<Long, double[]> sampleValues = accumulator.sampleValues();
    assertEquals(5 * 5, sampleValues.size());
  }

  @Test
  public void testGetRegressionData() {
    List<double[]> data = prepareData(10);
    RegressionMetricsAccumulator accumulator = new RegressionMetricsAccumulator(_causalRelation, 5, 3, 5, 1);
    for (double[] values : data) {
      accumulator.recordMetricValues(values);
    }
    accumulator.maybeRemoveOldValues();
    Set<MetricInfo> causalMetrics = _causalRelation.causalMetrics();
    DataForLinearRegression regressionData =
        accumulator.getRegressionData(causalMetrics);

    double[] dataForResultantMetric = regressionData.resultantMetricValues().get(_metricDef.metricInfo(METRIC1));
    assertEquals(10, dataForResultantMetric.length);
    int resultantMetricIndex = _causalRelation.indexes().get(_metricDef.metricInfo(METRIC1));
    for (int i = 0; i < 10; i++) {
      assertEquals(i + (float) resultantMetricIndex / 10, dataForResultantMetric[i], 1E-6);
    }

    double[][] causalMetricData = regressionData.causalMetricsValues();
    assertEquals(10, causalMetricData.length);
    for (int i = 0; i < 10; i++) {
      for (MetricInfo metricInfo : causalMetrics) {
        int causalMetricIndex = _causalRelation.indexes().get(metricInfo);
        assertEquals(i + (float) causalMetricIndex / 10,
                     causalMetricData[i][causalMetricIndex], 1E-6);
      }
    }
  }

  private List<double[]> prepareData(int numSamplesPerMetric) {
    List<double[]> data = new ArrayList<>();
    for (int i = 0; i < numSamplesPerMetric; i++) {
      double[] values = new double[_causalRelation.indexes().size()];
      for (Map.Entry<MetricInfo, Integer> entry : _causalRelation.indexes().entrySet()) {
        MetricInfo metricInfo = entry.getKey();
        values[metricInfo.id()] = i + (double) entry.getValue() / 10;
      }
      data.add(values);
    }
    return data;
  }
  
  
}
