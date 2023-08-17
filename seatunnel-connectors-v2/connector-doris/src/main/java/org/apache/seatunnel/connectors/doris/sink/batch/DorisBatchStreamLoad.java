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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.RestService;
import org.apache.seatunnel.connectors.doris.rest.models.RespContent;
import org.apache.seatunnel.connectors.doris.sink.HttpPutBuilder;
import org.apache.seatunnel.connectors.doris.sink.writer.LabelGenerator;
import org.apache.seatunnel.connectors.doris.util.BackendUtil;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.doris.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.seatunnel.connectors.doris.sink.LoadStatus.SUCCESS;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.seatunnel.connectors.doris.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/**
 * async stream load
 **/
@Slf4j
public class DorisBatchStreamLoad implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private String loadUrl;
    private String hostPort;
    private final String username;
    private final String password;
    private final String db;
    private final String table;
    private final Properties streamLoadProp;
    private BatchRecordBuffer buffer;
    private DorisConfig dorisConfig;
    private ExecutorService loadExecutorService;
    private LoadAsyncExecutor loadAsyncExecutor;
    private final BlockingQueue<BatchRecordBuffer> writeQueue;
    private final BlockingQueue<BatchRecordBuffer> readQueue;
    private final AtomicBoolean started;
    private final CountDownLatch loadCompletionLatch = new CountDownLatch(1);
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);
    private CloseableHttpClient httpClient = new HttpUtil().getHttpClient();
    private BackendUtil backendUtil;


    public DorisBatchStreamLoad(DorisConfig dorisConfig,
                                LabelGenerator labelGenerator) throws IOException {
        this.dorisConfig = dorisConfig;
        this.backendUtil = new BackendUtil(RestService.getBackendsV2(dorisConfig, log));
        this.hostPort = backendUtil.getAvailableBackend();
        String[] tableInfo = dorisConfig.getTableIdentifier().split("\\.");
        this.db = tableInfo[0];
        this.table = tableInfo[1];
        this.username = dorisConfig.getUsername();
        this.password = dorisConfig.getPassword();
        this.loadUrl = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        this.streamLoadProp = dorisConfig.getStreamLoadProps();

        this.labelGenerator = labelGenerator;
        this.lineDelimiter =
                streamLoadProp.getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT).getBytes();
        //init queue
        this.writeQueue = new ArrayBlockingQueue<>(dorisConfig.getFlushQueueSize());
        log.info("init RecordBuffer capacity {}, count {}", dorisConfig.getBufferFlushMaxBytes(), dorisConfig.getFlushQueueSize());
        for (int index = 0; index < dorisConfig.getFlushQueueSize(); index++) {
            this.writeQueue.add(new BatchRecordBuffer(this.lineDelimiter, dorisConfig.getBufferFlushMaxBytes()));
        }
        readQueue = new LinkedBlockingDeque<>();

        this.loadAsyncExecutor = new LoadAsyncExecutor();
        this.loadExecutorService =
                new ThreadPoolExecutor(1, 1, 0L,
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1),
                        new DefaultThreadFactory("streamload-executor"),
                        new ThreadPoolExecutor.AbortPolicy());
        this.started = new AtomicBoolean(true);
        this.loadExecutorService.execute(loadAsyncExecutor);
    }

    /**
     * write record into cache.
     *
     * @param record
     * @throws IOException
     */
    public synchronized void writeRecord(byte[] record) throws InterruptedException {
        checkFlushException();
        if (buffer == null) {
            buffer = takeRecordFromWriteQueue();
        }
        buffer.insert(record);
        //When it exceeds 80% of the byteSize,to flush, to avoid triggering bytebuffer expansion
        if (buffer.getBufferSizeBytes() >= dorisConfig.getBufferFlushMaxBytes() * 0.8
                || (dorisConfig.getBufferFlushMaxRows() != 0 && buffer.getNumOfRecords() >= dorisConfig.getBufferFlushMaxRows())) {
            flush(false);
        }
    }

    public synchronized void flush(boolean waitUtilDone) throws InterruptedException {
        checkFlushException();
        if (buffer != null && !buffer.isEmpty()) {
            buffer.setLabelName(labelGenerator.generateBatchLabel());
            BatchRecordBuffer tmpBuff = buffer;
            readQueue.put(tmpBuff);
            this.buffer = null;
        }

        if (waitUtilDone) {
            waitAsyncLoadFinish();
        }
    }

    private void putRecordToWriteQueue(BatchRecordBuffer buffer) {
        try {
            writeQueue.put(buffer);
        } catch (InterruptedException e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_ERROR, "Failed to recycle a buffer to queue");
        }
    }

    private BatchRecordBuffer takeRecordFromWriteQueue() {
        checkFlushException();
        try {
            return writeQueue.take();
        } catch (InterruptedException e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_ERROR, "Failed to take a buffer from queue");
        }
    }

    private void checkFlushException() {
        if (exception.get() != null) {
            throw new DorisConnectorException(DorisConnectorErrorCode.CHECK_FLUSH_ERROR, exception.get());
        }
    }

    private synchronized void waitAsyncLoadFinish() throws InterruptedException {
        for (int i = 0; i < dorisConfig.getFlushQueueSize() + 2; i++) {
            BatchRecordBuffer empty = takeRecordFromWriteQueue();
            readQueue.put(empty);
            log.info("write queue:{}", writeQueue.size());
        }
    }

    public synchronized void close() {

//        close async executor
        try {
            flush(true);
        } catch (InterruptedException e) {
            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_ERROR, e);
        }
        loadExecutorService.shutdown();
        this.started.set(false);
        // clear buffer
        this.writeQueue.clear();
        this.readQueue.clear();
    }

    class LoadAsyncExecutor implements Runnable {
        @Override
        public void run() {
            log.info("LoadAsyncExecutor start");
            while (started.get()) {
                BatchRecordBuffer buffer = null;
                try {
                    buffer = readQueue.poll(2000L, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        continue;
                    }
                    if (buffer.getLabelName() != null) {
                        load(buffer.getLabelName(), buffer);
                    }
                } catch (Exception e) {
                    log.error("worker running error", e);
                    exception.set(e);
                    break;
                } finally {
                    //Recycle buffer to avoid writer thread blocking
                    if (buffer != null) {
                        buffer.clear();
                        putRecordToWriteQueue(buffer);
                    }
                }

            }
            log.info("LoadAsyncExecutor stop");
        }

        /**
         * execute stream load
         */
        public void load(String label, BatchRecordBuffer buffer) throws IOException {
            refreshLoadUrl();
            ByteBuffer data = buffer.getData();
            ByteArrayEntity entity = new ByteArrayEntity(data.array(), data.arrayOffset(), data.limit());
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(loadUrl)
                    .baseAuth(username, password)
                    .setLabel(label)
                    .addCommonHeader()
                    .setEntity(entity)
                    .addHiddenColumns(dorisConfig.getEnableDelete())
                    .addProperties(dorisConfig.getStreamLoadProps());

            int retry = 0;
            while (retry <= dorisConfig.getMaxRetries()) {
                log.info("stream load started for {} on host {}", label, hostPort);
                try (CloseableHttpResponse response = httpClient.execute(putBuilder.build())) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode == 200 && response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        log.info("load Result {}", loadResult);
                        RespContent respContent = OBJECT_MAPPER.readValue(loadResult, RespContent.class);
                        if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                            String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
                            throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_ERROR, errMsg);
                        } else {
                            return;
                        }
                    }
                    log.error("stream load failed with {}, reason {}, to retry", hostPort, response.getStatusLine().toString());
                } catch (Exception ex) {
                    if (retry == dorisConfig.getMaxRetries()) {
                        throw new DorisConnectorException(DorisConnectorErrorCode.BATCH_LOAD_ERROR, ex);
                    }
                    log.error("stream load error with {}, to retry, cause by", hostPort, ex);

                }
                retry++;
                // get available backend retry
                refreshLoadUrl();
                putBuilder.setUrl(loadUrl);
            }
        }

        private void refreshLoadUrl() {
            hostPort = backendUtil.getAvailableBackend();
            loadUrl = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String name) {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
