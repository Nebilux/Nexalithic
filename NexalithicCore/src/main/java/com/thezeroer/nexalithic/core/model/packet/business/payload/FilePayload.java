package com.thezeroer.nexalithic.core.model.packet.business.payload;

import com.thezeroer.nexalithic.core.infra.buffer.LoopBuffer;
import com.thezeroer.nexalithic.core.messaging.task.visual.TransferSnapshot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * 文件有效载荷
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/02
 * @version 1.0.0
 */
public class FilePayload extends AbstractPayload<File> {
    public static final long UID = 2;
    public static final String TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"), "nexalithic").toAbsolutePath().toString();
    private volatile String sourceFileName;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private boolean metaProcessed;
    private volatile byte[] metaBytes;

    public FilePayload() {}
    public FilePayload(File file) throws FileNotFoundException {
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        value = file;
        sourceFileName = file.getName();
        metaBytes = sourceFileName.getBytes(StandardCharsets.UTF_8);
        totalSize = Integer.BYTES + metaBytes.length + file.length();
    }

    public String getSourceFileName() {
        return sourceFileName;
    }

    @Override
    public int encode(LoopBuffer.LimitedWritableView output) throws IOException {
        int length = 0;
        if (!metaProcessed) {
            if (output.remaining() < metaBytes.length) {
                return 0;
            }
            output.putInt(metaBytes.length);
            output.putBytes(metaBytes);
            length += Integer.BYTES + metaBytes.length;
            metaProcessed = true;
        }
        length += output.readFrom(fileChannel);
        processedSize += length;
        return length;
    }

    @Override
    public int decode(LoopBuffer.LimitedReadableView input) throws IOException {
        int length = 0;
        if (!metaProcessed) {
            metaBytes = input.getBytes(input.getInt());
            sourceFileName = new String(metaBytes, StandardCharsets.UTF_8);
            length += Integer.BYTES + metaBytes.length;
            metaProcessed = true;
        }
        length += input.writeTo(fileChannel);
        processedSize += length;
        return length;
    }

    @Override
    public AbstractPayload<File> duplicate() {
        try {
            FilePayload clone = new FilePayload();
            clone.value = this.value;
            clone.sourceFileName = this.sourceFileName;
            clone.metaBytes = this.metaBytes;
            clone.totalSize = this.totalSize;
            clone.processedSize = 0;
            clone.metaProcessed = false;
            return clone;
        } catch (Exception e) {
            throw new RuntimeException("Failed to duplicate FilePayload", e);
        }
    }

    @Override
    public void prepareEncode() throws IOException {
        randomAccessFile = new RandomAccessFile(value, "r");
        fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public void prepareDecode(long totalSize) throws IOException {
        super.prepareDecode(totalSize);
        value = Interior.createTempFile();
        if (!value.exists()) {
            value.createNewFile();
        }
        randomAccessFile = new RandomAccessFile(value, "rw");
        fileChannel = randomAccessFile.getChannel();
    }

    @Override
    public void release() {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ignored) {}
    }

    @Override
    public long getPayloadUID() {
        return UID;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[SrcFileName: " + sourceFileName + ", Size:" + TransferSnapshot.formatSize(totalSize - metaBytes.length) + ", CurrentPath: " + value.getAbsolutePath() + "]";
    }

    private static class Interior {

        public static File createTempFile() {
            return new File(TEMP_DIR, UUID.randomUUID() + ".tmp");
        }

        static {
            try {
                Files.createDirectories(Path.of(TEMP_DIR));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
