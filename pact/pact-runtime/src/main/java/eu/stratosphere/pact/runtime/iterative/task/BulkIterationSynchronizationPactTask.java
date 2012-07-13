/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.Callback;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkIterationSynchronizationPactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {

  private boolean terminated = false;
  private int numIterations = 0;

  private static final Log log = LogFactory.getLog(BulkIterationSynchronizationPactTask.class);

  @Override
  protected ReaderInterruptionBehavior readerInterruptionBehavior() {
    return ReaderInterruptionBehaviors.FALSE_ON_INTERRUPT;
  }

  @Override
  protected int numberOfEventsUntilInterrupt() {
    return numberOfConnectedHeads();
  }

  private int numberOfConnectedHeads() {
    //TODO return number of connected workers
    return 1;
  }

  @Override
  public void invoke() throws Exception {

    final AtomicInteger nonTerminatedHeadsCounter = new AtomicInteger(numberOfConnectedHeads());

    listenToTermination(new Callback<TerminationEvent>() {
      @Override
      public void execute(TerminationEvent event) throws Exception {
        int numNonTerminatedHeads = nonTerminatedHeadsCounter.decrementAndGet();
        if (numNonTerminatedHeads == 0) {
          terminated = true;
        }
      }
    });

    while (!terminated) {

      log.info("starting iteration [" + numIterations + "] [" + System.currentTimeMillis() + "]");
      if (numIterations > 0) {
        reinstantiateDriver();
      }

      super.invoke();

      log.info("signaling that all workers are done in iteration [" + numIterations + "] " +
          "[" + System.currentTimeMillis() + "]");
      signalAllWorkersDone();

      log.info("finishing iteration [" + numIterations + "] [" + System.currentTimeMillis() + "]");
      numIterations++;
    }
  }

  private void signalAllWorkersDone() throws IOException, InterruptedException {
    AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent();
    for (int inputGateIndex = 0; inputGateIndex < getEnvironment().getNumberOfInputGates(); inputGateIndex++) {
      getEnvironment().getInputGate(inputGateIndex).publishEvent(allWorkersDoneEvent);
    }
  }
}
