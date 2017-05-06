/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.virtual.ExtractionFnVirtualColumn;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.SelectProjection;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class SelectRules
{
  private static final List<RelOptRule> RULES = ImmutableList.of(
      new DruidSelectProjectionRule(),
      new DruidSelectSortRule()
  );

  private SelectRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return RULES;
  }

  static class DruidSelectProjectionRule extends RelOptRule
  {
    private DruidSelectProjectionRule()
    {
      super(operand(Project.class, operand(DruidRel.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);

      return druidRel.getQueryBuilder().getSelectProjection() == null
             && druidRel.getQueryBuilder().getGrouping() == null
             && druidRel.getQueryBuilder().getLimitSpec() == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Project project = call.rel(0);
      final DruidRel druidRel = call.rel(1);

      // Only push in projections that can be used by the Select query.
      // Leave anything more complicated to DruidAggregateProjectRule for possible handling in a GroupBy query.

      final RowSignature sourceRowSignature = druidRel.getSourceRowSignature();
      final List<VirtualColumn> virtualColumns = Lists.newArrayList();
      final List<String> rowOrder = Lists.newArrayList();

      int outputNameCounter = 0;
      for (int i = 0; i < project.getRowType().getFieldCount(); i++) {
        final RexNode rexNode = project.getChildExps().get(i);
        final RowExtraction rex = Expressions.toRowExtraction(
            druidRel.getPlannerContext(),
            sourceRowSignature.getRowOrder(),
            rexNode
        );

        if (rex == null) {
          return;
        }

        final String column = rex.getColumn();
        final ExtractionFn extractionFn = rex.getExtractionFn();

        if (extractionFn == null) {
          rowOrder.add(column);
        } else {
          do {
            outputNameCounter++;
          } while (sourceRowSignature.getColumnType("v" + outputNameCounter) != null);
          final String outputName = "v" + outputNameCounter;
          virtualColumns.add(new ExtractionFnVirtualColumn(outputName, column, extractionFn));
          rowOrder.add(outputName);
        }
      }

      call.transformTo(
          druidRel.withQueryBuilder(
              druidRel.getQueryBuilder()
                      .withSelectProjection(
                          new SelectProjection(project, VirtualColumns.create(virtualColumns)),
                          rowOrder
                      )
          )
      );
    }
  }

  static class DruidSelectSortRule extends RelOptRule
  {
    private DruidSelectSortRule()
    {
      super(operand(Sort.class, operand(DruidRel.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);

      return druidRel.getQueryBuilder().getGrouping() == null
             && druidRel.getQueryBuilder().getLimitSpec() == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Sort sort = call.rel(0);
      final DruidRel druidRel = call.rel(1);

      final DefaultLimitSpec limitSpec = GroupByRules.toLimitSpec(druidRel.getQueryBuilder().getRowOrder(), sort);
      if (limitSpec == null) {
        return;
      }

      // Scan query can handle limiting but not sorting, so avoid applying this rule if there is a sort.
      if (limitSpec.getColumns().isEmpty()) {
        call.transformTo(
            druidRel.withQueryBuilder(
                druidRel.getQueryBuilder()
                        .withLimitSpec(limitSpec)
            )
        );
      }
    }
  }
}
