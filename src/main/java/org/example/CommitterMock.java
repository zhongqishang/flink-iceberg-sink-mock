package org.example;

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class CommitterMock extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<WriteResult, Void>, BoundedOneInput {
    private static final Logger LOG = LoggerFactory.getLogger(CommitterMock.class);

    private ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    public void endInput() throws Exception {}

    @Override
    public void processElement(StreamRecord<WriteResult> element) throws Exception {
        LOG.info("WriteResult : {}", element.getValue());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        LOG.info(
                "snapshotState :{}, Thread Name : {}",
                context.getCheckpointId(),
                Thread.currentThread().getName());
        Thread.sleep(random.nextLong(2000L));
        super.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        Thread.sleep(random.nextLong(20000L));
        LOG.info(
                "notifyCheckpointComplete :{}, Thread Name : {}",
                checkpointId,
                Thread.currentThread().getName());
    }
}
