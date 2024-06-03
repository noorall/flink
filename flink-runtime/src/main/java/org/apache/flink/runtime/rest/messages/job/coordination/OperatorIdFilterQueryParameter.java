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

package org.apache.flink.runtime.rest.messages.job.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.util.StringUtils;

/** {@link MessageQueryParameter} for filtering stream node id. */
public class OperatorIdFilterQueryParameter extends MessageQueryParameter<OperatorID> {

    OperatorIdFilterQueryParameter() {
        super("operatorid", MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public OperatorID convertStringToValue(String value) throws ConversionException {
        try {
            return new OperatorID(StringUtils.hexStringToByte(value));
        } catch (IllegalArgumentException iae) {
            throw new ConversionException("Not a valid operator Id: " + value, iae);
        }
    }

    @Override
    public String convertValueToString(OperatorID value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "string value that identifies an operator.";
    }
}
