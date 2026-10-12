/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.model.packet.business.payload;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacketException;

import java.io.File;
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
 * @author Reonvia
 * @since 0.1.0
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
    public FilePayload(File file) throws FilePayloadException {
        if (!file.exists()) {
            throw FilePayloadException.missingSource(file);
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
            throw BusinessPacketException.failedToDuplicate(FilePayload.class, e);
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
