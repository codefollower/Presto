/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestMultipleDistinctAggregationToMarkDistinct
        extends BaseRuleTest
{
    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.variable("output1"), expression("count(input1)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.variable("output2"), expression("count(input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.variable("input1"),
                                        p.variable("input2")))))
                .doesNotFire();
    }

    @Test
    public void testSingleDistinct()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.variable("output1"), expression("count(DISTINCT input1)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.variable("input1"),
                                        p.variable("input2")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.variable("output1"), expression("count(DISTINCT input)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.variable("output2"), expression("sum(DISTINCT input)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.variable("input")))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.variable("output1"), expression("count(DISTINCT input1) filter (where input2 > 0)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.variable("output2"), expression("count(DISTINCT input2) filter (where input1 > 0)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.variable("input1"),
                                        p.variable("input2")))))
                .doesNotFire();

        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.variable("output1"), expression("count(DISTINCT input1) filter (where input2 > 0)"), ImmutableList.of(BIGINT))
                        .addAggregation(p.variable("output2"), expression("count(DISTINCT input2)"), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.variable("input1"),
                                        p.variable("input2")))))
                .doesNotFire();
    }
}
