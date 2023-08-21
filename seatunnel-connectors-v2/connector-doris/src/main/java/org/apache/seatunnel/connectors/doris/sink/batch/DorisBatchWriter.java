// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.seatunnel.connectors.doris.sink.batch;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.serialize.DorisSerializer;
import org.apache.seatunnel.connectors.doris.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.doris.sink.writer.LabelGenerator;
import org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Slf4j
public class DorisBatchWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private DorisBatchStreamLoad batchStreamLoad;
    private final DorisConfig dorisConfig;
    private final String labelPrefix;
    private final DorisSerializer serializer;
    private final LabelGenerator labelGenerator;
    private final long flushIntervalMs;
    private final transient ScheduledExecutorService scheduledExecutorService;
    private transient volatile Exception flushException = null;

    public DorisBatchWriter(SinkWriter.Context context, DorisConfig dorisConfig, SeaTunnelRowType seaTunnelRowType) {
        this.dorisConfig = dorisConfig;
        this.labelPrefix = dorisConfig.getLabelPrefix() + "_" + context.getIndexOfSubtask();
        this.labelGenerator = new LabelGenerator(labelPrefix, false);
        this.serializer = createSerializer(dorisConfig, seaTunnelRowType);
        this.flushIntervalMs = dorisConfig.getBufferFlushIntervalMs();
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("stream-load-flush-interval").build());
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkFlushException();
        byte[] serialize = serializer.serialize(element);
        if (Objects.isNull(serialize)) {
            //ddl record
            return;
        }
        try {
            batchStreamLoad.writeRecord(serialize);
        } catch (InterruptedException e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_INTERRUPTED_ERROR, e);
        }

    }


    @Override
    public void close() throws IOException {
        log.info("DorisBatchWriter Close");
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        batchStreamLoad.close();
    }

    public void initializeLoad() throws IOException {
        this.batchStreamLoad = new DorisBatchStreamLoad(dorisConfig, labelGenerator);
        // when uploading data in streaming mode, we need to regularly detect whether there are exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(this::intervalFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void intervalFlush() {
        try {
            log.info("interval flush triggered.");
            batchStreamLoad.flush(false);
        } catch (InterruptedException e) {
            flushException = e;
        }
    }

    private DorisSerializer createSerializer(
            DorisConfig dorisConfig, SeaTunnelRowType seaTunnelRowType) {
        return new SeaTunnelRowSerializer(
                dorisConfig
                        .getStreamLoadProps()
                        .getProperty(LoadConstants.FORMAT_KEY)
                        .toLowerCase(),
                seaTunnelRowType,
                dorisConfig.getStreamLoadProps().getProperty(LoadConstants.FIELD_DELIMITER_KEY),
                dorisConfig.getEnableDelete());
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new DorisConnectorException(DorisConnectorErrorCode.CHECK_FLUSH_ERROR, "Writing records to streamload failed.", flushException);
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            batchStreamLoad.flush(true);
        } catch (InterruptedException e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_INTERRUPTED_ERROR, e);
        }
        return super.prepareCommit();
    }
}
