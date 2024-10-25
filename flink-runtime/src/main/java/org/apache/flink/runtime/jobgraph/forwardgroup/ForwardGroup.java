/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph.forwardgroup;

/**
 * A forward group is a set of job vertices or stream nodes connected via forward edges.
 * Parallelisms of all job vertices in the same {@link ForwardGroup} must be the same.
 */
public interface ForwardGroup {

    /**
     * Sets the parallelism for this forward group.
     *
     * @param parallelism the parallelism to set.
     */
    void setParallelism(int parallelism);

    /**
     * Returns if parallelism has been decided for this forward group.
     *
     * @return is parallelism decided for this forward group.
     */
    boolean isParallelismDecided();

    /**
     * Returns the parallelism for this forward group.
     *
     * @return parallelism for this forward group.
     */
    int getParallelism();

    /**
     * Returns if max parallelism has been decided for this forward group.
     *
     * @return is max parallelism decided for this forward group.
     */
    boolean isMaxParallelismDecided();

    /**
     * Returns the max parallelism for this forward group.
     *
     * @return max parallelism for this forward group.
     */
    int getMaxParallelism();

    /**
     * Returns the size for this forward group.
     *
     * @return size for this forward group.
     */
    int size();
}
