/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.linq4j;

import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EnumerableTest {

  @Test
  public void name() {
    final Enumerable<Integer> input = Linq4j.asEnumerable(IntStream.range(0, 100).boxed().collect(Collectors.toList()));

    final SpecialEnumerable<Integer> integers = new SpecialEnumerable<>(input, 5, 1);
    final Enumerator<State<Integer>> enumerator = integers.enumerator();

    final List<State<Integer>> results = new ArrayList<>();
    while (enumerator.moveNext()) {
      final State<Integer> current = enumerator.current();
      results.add(current);
      System.out.println(current);
    }

    assertEquals(100, results.size());
    assertEquals( 0, (int)results.get(0).get());
    assertEquals( null, results.get(0).get(-2));
    assertEquals( 1, (int)results.get(0).get(1));
    assertEquals( 99, (int)results.get(99).get());
    assertEquals( 97, (int)results.get(99).get(-2));
    assertEquals( null, results.get(99).get(1));
  }

  public static class SpecialEnumerable<E> extends AbstractEnumerable<State<E>> {

    private final Enumerable<E> input;
    private final int history;
    private final int future;

    public SpecialEnumerable(Enumerable<E> input, int history, int future) {
      this.input = input;
      this.history = history;
      this.future = future;
    }

    @Override public Enumerator<State<E>> enumerator() {
      return new SpecialEnumerator<>(input.enumerator(), history, future);
    }
  }

  public static class SpecialEnumerator<E> implements Enumerator<State<E>> {

    private final Enumerator<E> enumerator;
    private final int future;
    private final State<E> state;
    private final AtomicInteger prevCounter;
    private final AtomicInteger postCounter;

    public SpecialEnumerator(Enumerator<E> enumerator, int history, int future) {
      this.enumerator = enumerator;
      this.future = future;
      this.state = new State<>(history, future);
      this.prevCounter = new AtomicInteger(future);
      this.postCounter = new AtomicInteger(future);
    }

    @Override public State<E> current() {
      return this.state.copy(); //enumerator.current();
    }

    @Override public boolean moveNext() {
      if (prevCounter.get() > 0) {
        boolean lastMove = false;
        while (prevCounter.getAndDecrement() >= 0) {
          lastMove = moveNextInternal();
        }
        return lastMove;
      } else {
        return moveNextInternal();
      }
    }

    private boolean moveNextInternal() {
      final boolean moveNext = enumerator.moveNext();
      if (moveNext) {
        state.add(enumerator.current());
        return true;
      } else {
        // Check if we have to add "history" additional values
        if (postCounter.getAndDecrement() > 0) {
          state.add(null);
          return true;
        }
      }
      return false;
    }

    @Override public void reset() {
      enumerator.reset();
    }

    @Override public void close() {
      enumerator.close();
    }
  }

  public static class State<E> {

    private final int history;
    private final int future;
    // Index:      0   1   2   3   4
    // Idea       -2  -1   0  +1  +2
    int offset = 0;
    private Object[] values;

    public State(int history, int future) {
      this.history = history;
      this.future = future;
      this.values = new Object[history + future + 1];
    }

    private State(int history, int future, int offset, Object[] values) {
      this.history = history;
      this.future = future;
      this.offset = offset;
      this.values = values;
    }

    public E get() {
      return get(0);
    }

    public E get(int offset) {
      if (offset < 0 && offset < -1*history) {
        throw new IllegalArgumentException("History can only go back " + history + " points in time, you wanted " + Math.abs(offset));
      }
      if (offset > 0 && offset > future) {
        throw new IllegalArgumentException("Future can only see next " + future + " points in time, you wanted " + offset);
      }
      return (E)this.values[plus(offset - 1 - future)];
    }

    public void add(E current) {
      values[offset] = current;
      this.offset = plus(1);
    }

    private int plus(int steps) {
      if (steps < 0) {
        return minus(Math.abs(steps));
      }
      return (offset + steps) % (history + future + 1);
    }

    private int minus(int steps) {
      assert steps >= 0;
      int r = (offset - steps);
      if (r < 0) {
        return r + (history + future + 1);
      } else {
        return r;
      }
    }

    @Override public String toString() {
      return Arrays.toString(this.values);
    }

    public State<E> copy() {
      return new State<>(history, future, offset, values.clone());
    }
  }
}
