package org.example;

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class WriterMock extends AbstractStreamOperator<WriteResult>
        implements OneInputStreamOperator<String, WriteResult>, BoundedOneInput {
    private static final Logger LOG = LoggerFactory.getLogger(WriterMock.class);

    private ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    public void endInput() throws Exception {}

    @Override
    public void processElement(StreamRecord<String> element) throws Exception {}

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        LOG.info(
                "prepareSnapshotPreBarrier : {}, IndexInSubtask : {}",
                checkpointId,
                getContainingTask().getIndexInSubtaskGroup());
        Thread.sleep(random.nextLong(5000L));
        output.collect(new StreamRecord<>(new WriteResult(checkpointId, "result")));
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }
}
