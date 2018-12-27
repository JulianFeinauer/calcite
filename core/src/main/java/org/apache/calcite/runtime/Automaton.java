/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.runtime.mr.Tuple;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.Set;

/** A finite-state automaton.
 *
 * <p>It is used to implement the {@link org.apache.calcite.rel.core.Match}
 * relational expression (for the {@code MATCH_RECOGNIZE} clause in SQL).
 *
 * @see Pattern
 * @see AutomatonBuilder
 */
public class Automaton {
  final State startState;
  final State endState;
  private final ImmutableList<SymbolTransition> transitions;
  private final ImmutableList<EpsilonTransition> epsilonTransitions;
  final ImmutableList<String> symbolNames;

  /** Use an {@link AutomatonBuilder}. */
  Automaton(State startState, State endState,
      ImmutableList<SymbolTransition> transitions,
      ImmutableList<EpsilonTransition> epsilonTransitions,
      ImmutableList<String> symbolNames) {
    this.startState = Objects.requireNonNull(startState);
    this.endState = Objects.requireNonNull(endState);
    this.transitions = Objects.requireNonNull(transitions);
    this.epsilonTransitions = epsilonTransitions;
    this.symbolNames = Objects.requireNonNull(symbolNames);
  }

  /** Returns the set of states, represented as a bit set, that the graph is
   * in when starting from {@code state} and receiving {@code symbol}.
   *
   * @param fromState Initial state
   * @param symbol Symbol received
   * @param bitSet Set of successor states (output)
   */
  void successors(int fromState, int symbol,
      ImmutableBitSet.Builder bitSet) {
    for (SymbolTransition transition : transitions) {
      if (transition.fromState.id == fromState
          && transition.symbol == symbol) {
        epsilonSuccessors(transition.toState.id, bitSet);
      }
    }
  }

  void epsilonSuccessors(int state, ImmutableBitSet.Builder bitSet) {
    bitSet.set(state);
    for (EpsilonTransition transition : epsilonTransitions) {
      if (transition.fromState.id == state) {
        if (!bitSet.get(transition.toState.id)) {
          epsilonSuccessors(transition.toState.id, bitSet);
        }
      }
    }
  }

  /** Node in the finite-state automaton. A state has a number of
   * transitions to other states, each labeled with the symbol that
   * causes that transition. */
  static class State {
    final int id;

    State(int id) {
      this.id = id;
    }
  }

  /** Transition from one state to another in the finite-state automaton. */
  abstract static class Transition {
    final State fromState;
    final State toState;

    Transition(State fromState, State toState) {
      this.fromState = Objects.requireNonNull(fromState);
      this.toState = Objects.requireNonNull(toState);
    }
  }

  /** A transition caused by reading a symbol. */
  static class SymbolTransition extends Transition {
    final int symbol;

    SymbolTransition(State fromState, State toState, int symbol) {
      super(fromState, toState);
      this.symbol = symbol;
    }

    @Override public String toString() {
      return symbol + ":" + fromState.id + "->" + toState.id;
    }
  }

  /** A transition that may happen without reading a symbol. */
  static class EpsilonTransition extends Transition {
    EpsilonTransition(State fromState, State toState) {
      super(fromState, toState);
    }

    @Override public String toString() {
      return "epsilon:" + fromState.id + "->" + toState.id;
    }
  }
}

// End Automaton.java
