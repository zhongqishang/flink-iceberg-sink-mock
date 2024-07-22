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

package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/** Skeleton for a Flink DataStream Job. */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(1000);
        // env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        // env.getCheckpointConfig().setForceUnalignedCheckpoints(true);

        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;

        DataGeneratorSource<String> generatorSource =
                new DataGeneratorSource<>(
                        generatorFunction,
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.STRING);

        DataStreamSource<String> streamSource =
                env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

        SingleOutputStreamOperator<WriteResult> writerStream =
                streamSource
                        .transform(
                                "WriterMock",
                                TypeInformation.of(WriteResult.class),
                                new WriterMock())
                        .setParallelism(4)
                        .setMaxParallelism(4);

        SingleOutputStreamOperator<Void> committerStream =
                writerStream
                        .transform("CommitterMock", Types.VOID, new CommitterMock())
                        .setParallelism(1)
                        .setMaxParallelism(1);

        committerStream.addSink(new DiscardingSink<>()).name("discardingSink");

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
