/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.autoscaler.utils;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

/** Jackson serializer module for {@link JobVertexID}. */
public class JobVertexSerDeModule extends SimpleModule {

    public JobVertexSerDeModule() {
        this.addKeySerializer(JobVertexID.class, new JobVertexIdKeySerializer());
        this.addKeyDeserializer(JobVertexID.class, new JobVertexIdKeyDeserializer());
    }

    private static class JobVertexIdKeySerializer extends JsonSerializer<JobVertexID> {
        @Override
        public void serialize(JobVertexID value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {

            jgen.writeFieldName(value.toHexString());
        }
    }

    private static class JobVertexIdKeyDeserializer extends KeyDeserializer {
        @Override
        public Object deserializeKey(String s, DeserializationContext deserializationContext) {
            return JobVertexID.fromHexString(s);
        }
    }
}
