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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EnumerableTest {

  @Test
  public void name() {
    final Enumerable<Integer> input = Linq4j.asEnumerable(IntStream.range(0, 100).boxed().collect(Collectors.toList()));

    final MemoryEnumerable<Integer> integers = new MemoryEnumerable<>(input, 5, 1);
    final Enumerator<MemoryFactory.Memory<Integer>> enumerator = integers.enumerator();

    final List<MemoryFactory.Memory<Integer>> results = new ArrayList<>();
    while (enumerator.moveNext()) {
      final MemoryFactory.Memory<Integer> current = enumerator.current();
      results.add(current);
      System.out.println(current);
    }

    assertEquals(100, results.size());
    // First entry
    assertEquals( 0, (int)results.get(0).get());
    assertEquals( 1, (int)results.get(0).get(1));
    assertNull(results.get(0).get(-2));
    // Last entry
    assertEquals( 99, (int)results.get(99).get());
    assertEquals( 97, (int)results.get(99).get(-2));
    assertNull(results.get(99).get(1));
  }

  @Test
  public void testFiniteInteger() {
    final FiniteInteger finiteInteger = new FiniteInteger(4, 5);
    assertEquals("4 (5)", finiteInteger.toString());

    final FiniteInteger plus = finiteInteger.plus(1);
    assertEquals("0 (5)", plus.toString());

    final FiniteInteger minus = finiteInteger.plus(-6);
    assertEquals("3 (5)", minus.toString());
  }

  public static class MemoryEnumerable<E> extends AbstractEnumerable<MemoryFactory.Memory<E>> {

    private final Enumerable<E> input;
    private final int history;
    private final int future;

    public MemoryEnumerable(Enumerable<E> input, int history, int future) {
      this.input = input;
      this.history = history;
      this.future = future;
    }

    @Override public Enumerator<MemoryFactory.Memory<E>> enumerator() {
      return new MemoryEnumerator<>(input.enumerator(), history, future);
    }
  }

  public static class MemoryEnumerator<E> implements Enumerator<MemoryFactory.Memory<E>> {

    private final Enumerator<E> enumerator;
    private final MemoryFactory<E> memoryFactory;
    private final AtomicInteger prevCounter;
    private final AtomicInteger postCounter;

    public MemoryEnumerator(Enumerator<E> enumerator, int history, int future) {
      this.enumerator = enumerator;
      this.memoryFactory = new MemoryFactory<>(history, future);
      this.prevCounter = new AtomicInteger(future);
      this.postCounter = new AtomicInteger(future);
    }

    @Override public MemoryFactory.Memory<E> current() {
      return this.memoryFactory.create(); //enumerator.current();
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
        memoryFactory.add(enumerator.current());
        return true;
      } else {
        // Check if we have to add "history" additional values
        if (postCounter.getAndDecrement() > 0) {
          memoryFactory.add(null);
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

  /**
   * Contains the State and changes internally.
   * with the {@link #create()} method one can get immutable Snapshots.
   * @param <E> Type of the base Object
   */
  public static class MemoryFactory<E> {

    private final int history;
    private final int future;
    // Index:      0   1   2   3   4
    // Idea       -2  -1   0  +1  +2
    FiniteInteger offset;
    private Object[] values;

    public MemoryFactory(int history, int future) {
      this.history = history;
      this.future = future;
      this.values = new Object[history + future + 1];
      this.offset = new FiniteInteger(0, history + future + 1);
    }

    public void add(E current) {
      values[offset.get()] = current;
      this.offset = offset.plus(1);
    }

    public Memory<E> create() {
      return new Memory<>(history, future, offset, values.clone());
    }

    public static class Memory<E> {

      private final int history;
      private final int future;
      private final FiniteInteger offset;
      private final Object[] values;

      public Memory(int history, int future, FiniteInteger offset, Object[] values) {
        this.history = history;
        this.future = future;
        this.offset = offset;
        this.values = values;
      }

      @Override public String toString() {
        return Arrays.toString(this.values);
      }

      public E get() {
        return get(0);
      }

      public E get(int position) {
        if (position < 0 && position < -1 * history) {
          throw new IllegalArgumentException("History can only go back " + history + " points in time, you wanted " + Math.abs(position));
        }
        if (position > 0 && position > future) {
          throw new IllegalArgumentException("Future can only see next " + future + " points in time, you wanted " + position);
        }
        return (E) this.values[this.offset.plus(position - 1 - future).get()];
      }
    }
  }

  /**
   * Represents a finite integer, i.e., calculation modulo.
   * This object is immutable and all operations create a new object.
   */
  public static class FiniteInteger {

    private final int value;
    private final int modul;

    public FiniteInteger(int value, int modul) {
      this.value = value;
      this.modul = modul;
    }

    public int get() {
      return this.value;
    }

    public FiniteInteger plus(int operand) {
      if (operand < 0) {
        return minus(Math.abs(operand));
      }
      return new FiniteInteger((value + operand) % modul, modul);
    }

    public FiniteInteger minus(int operand) {
      assert operand >= 0;
      int r = (value - operand);
      while (r < 0) {
        r = r + modul;
      }
      return new FiniteInteger(r, modul);
    }

    @Override public String toString() {
      return value + " (" + modul + ')';
    }
  }
}
