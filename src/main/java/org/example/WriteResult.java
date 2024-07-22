package org.example;

import java.io.Serializable;

public class WriteResult implements Serializable {
    private long checkpointId;
    private String str;

    public WriteResult(long checkpointId, String str) {
        this.checkpointId = checkpointId;
        this.str = str;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    @Override
    public String toString() {
        return "WriteResult{" + "checkpointId=" + checkpointId + ", str='" + str + '\'' + '}';
    }
}
