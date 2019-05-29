package com.kafka;

public class FileStatus{
    private Long lastModifiedTime;
    private Long fileSize;
    private boolean isWrited;

    public FileStatus(Long lastModifiedTime, Long fileSize, boolean isWrited) {
        this.lastModifiedTime = lastModifiedTime;
        this.fileSize = fileSize;
        this.isWrited = isWrited;
    }

    public Long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public boolean isWrited() {
        return isWrited;
    }

    public void setLastModifiedTime(Long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public void setWrited(boolean writed) {
        isWrited = writed;
    }
}
