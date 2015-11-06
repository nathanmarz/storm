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
package org.apache.storm.sql.compiler;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.stream.Delta;

public abstract class PostOrderRelNodeVisitor<T> {
  public final T traverse(RelNode n) throws Exception {
    for (RelNode input : n.getInputs()) {
      traverse(input);
    }

    if (n instanceof Aggregate) {
      return visitAggregate((Aggregate) n);
    } else if (n instanceof Calc) {
      return visitCalc((Calc) n);
    } else if (n instanceof Collect) {
      return visitCollect((Collect) n);
    } else if (n instanceof Correlate) {
      return visitCorrelate((Correlate) n);
    } else if (n instanceof Delta) {
      return visitDelta((Delta) n);
    } else if (n instanceof Exchange) {
      return visitExchange((Exchange) n);
    } else if (n instanceof Project) {
      return visitProject((Project) n);
    } else if (n instanceof Filter) {
      return visitFilter((Filter) n);
    } else if (n instanceof Sample) {
      return visitSample((Sample) n);
    } else if (n instanceof Sort) {
      return visitSort((Sort) n);
    } else if (n instanceof TableModify) {
      return visitTableModify((TableModify) n);
    } else if (n instanceof TableScan) {
      return visitTableScan((TableScan) n);
    } else if (n instanceof Uncollect) {
      return visitUncollect((Uncollect) n);
    } else if (n instanceof Window) {
      return visitWindow((Window) n);
    } else {
      return defaultValue(n);
    }
  }

  public T visitAggregate(Aggregate aggregate) throws Exception {
    return defaultValue(aggregate);
  }

  public T visitCalc(Calc calc) throws Exception {
    return defaultValue(calc);
  }

  public T visitCollect(Collect collect) throws Exception {
    return defaultValue(collect);
  }

  public T visitCorrelate(Correlate correlate) throws Exception {
    return defaultValue(correlate);
  }

  public T visitDelta(Delta delta) throws Exception {
    return defaultValue(delta);
  }

  public T visitExchange(Exchange exchange) throws Exception {
    return defaultValue(exchange);
  }

  public T visitProject(Project project) throws Exception {
    return defaultValue(project);
  }

  public T visitFilter(Filter filter) throws Exception {
    return defaultValue(filter);
  }

  public T visitSample(Sample sample) throws Exception {
    return defaultValue(sample);
  }

  public T visitSort(Sort sort) throws Exception {
    return defaultValue(sort);
  }

  public T visitTableModify(TableModify modify) throws Exception {
    return defaultValue(modify);
  }

  public T visitTableScan(TableScan scan) throws Exception {
    return defaultValue(scan);
  }

  public T visitUncollect(Uncollect uncollect) throws Exception {
    return defaultValue(uncollect);
  }

  public T visitWindow(Window window) throws Exception {
    return defaultValue(window);
  }

  public T defaultValue(RelNode n) {
    return null;
  }
}
