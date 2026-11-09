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

package com.nebilux.nexalithic.core.model.business.packet.builtin;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodec;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 文件有效载荷
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class FilePart extends BusinessPacket.Part<File> {
    public static final String TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"), "nexalithic").toAbsolutePath().toString();
    private volatile String sourceFileName;

    public FilePart(File file) throws FilePartException {
        super(file);
        if (!file.exists()) {
            throw FilePartException.missingSource(file);
        }
        sourceFileName = file.getName();
    }

    private FilePart(File file, String sourceFileName) {
        super(file);
        this.sourceFileName = sourceFileName;
    }

    public String getSourceFileName() {
        return sourceFileName;
    }

    @Override
    public long logicalSize() {
        return value.length();
    }

    @Override
    public FilePart duplicate() {
        return new FilePart(value, sourceFileName);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[SrcFileName: " + sourceFileName + ", Size:"
                + TransferSnapshot.formatSize(value.length()) + ", CurrentPath: " + value.getAbsolutePath() + "]";
    }

    /**
     * 编解码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static final class Codec implements PartCodec<FilePart> {
        public static final int UID = 2;

        @Override
        public int id() {
            return UID;
        }

        @Override
        public Class<FilePart> clazz() {
            return FilePart.class;
        }

        @Override
        public Encoder<FilePart> encoder() {
            return new FilePartEncoder();
        }

        @Override
        public Decoder<FilePart> decoder() {
            return new FilePartDecoder();
        }

        private static final class FilePartEncoder implements Encoder<FilePart> {
            private File source;
            private byte[] name;
            private long size;
            private long processed;
            private int prefixWritten;
            private int nameWritten;
            private RandomAccessFile file;
            private FileChannel channel;

            @Override
            public long prepare(FilePart part) throws IOException {
                clear();
                source = part.value();
                if (source == null || !source.isFile()) {
                    throw new IOException("Invalid source file");
                }
                String sourceName = part.sourceFileName == null ? source.getName() : part.sourceFileName;
                name = sourceName.getBytes(StandardCharsets.UTF_8);
                if (name.length > 65535) {
                    throw new IOException("File name is too long: " + name.length);
                }
                try {
                    size = Math.addExact(Math.addExact(Integer.BYTES, name.length), source.length());
                } catch (ArithmeticException failure) {
                    throw new IOException("File payload length overflow", failure);
                }
                return size;
            }

            @Override
            public void encode(LoopBuffer.LimitedWritableView target) throws IOException {
                if (channel == null) {
                    file = new RandomAccessFile(source, "r");
                    channel = file.getChannel();
                }
                int written = 0;
                while (prefixWritten < Integer.BYTES && target.remaining() > 0) {
                    target.putByte((byte) (name.length >>> (24 - prefixWritten * 8)));
                    prefixWritten++;
                    written++;
                }
                if (prefixWritten == Integer.BYTES && nameWritten < name.length) {
                    int n = target.readFrom(name, nameWritten);
                    nameWritten += n;
                    written += n;
                }
                if (nameWritten == name.length && target.remaining() > 0 && processed + written < size) {
                    int n = target.readFrom(channel);
                    written += n;
                }
                processed += written;
                if (written == 0 && target.remaining() > 0 && processed < size) {
                    throw new IOException("Source file ended before declared payload length");
                }
            }

            @Override
            public boolean tryFinish() {
                return processed == size;
            }

            @Override
            public void clear() {
                if (file != null) {
                    try {
                        file.close();
                    } catch (IOException ignored) {
                    }
                }
                file = null;
                channel = null;
                source = null;
                name = null;
                size = 0;
                processed = 0;
                prefixWritten = 0;
                nameWritten = 0;
            }
        }

        private static final class FilePartDecoder implements Decoder<FilePart> {
            private long size;
            private long processed;
            private int prefixRead;
            private int nameLength;
            private byte[] name;
            private int nameRead;
            private File target;
            private RandomAccessFile file;
            private FileChannel channel;
            private boolean finished;

            @Override
            public void prepare(long size) throws IOException {
                clear();
                if (size < Integer.BYTES) {
                    throw new IOException("Invalid file payload length: " + size);
                }
                this.size = size;
                Files.createDirectories(Path.of(TEMP_DIR));
                target = Files.createTempFile(Path.of(TEMP_DIR), "", ".tmp").toFile();
                file = new RandomAccessFile(target, "rw");
                channel = file.getChannel();
            }

            @Override
            public void decode(LoopBuffer.LimitedReadableView source) throws IOException {
                int read = 0;
                while (prefixRead < Integer.BYTES && source.remaining() > 0) {
                    nameLength = (nameLength << 8) | Byte.toUnsignedInt(source.getByte());
                    prefixRead++;
                    read++;
                }
                if (prefixRead == Integer.BYTES && name == null) {
                    if (nameLength < 0 || nameLength > size - Integer.BYTES || nameLength > 65535) {
                        throw new IOException("Invalid file name length: " + nameLength);
                    }
                    name = new byte[nameLength];
                }
                if (name != null && nameRead < name.length) {
                    int n = source.writeTo(name, nameRead);
                    nameRead += n;
                    read += n;
                }
                if (name != null && nameRead == name.length && source.remaining() > 0) {
                    read += source.writeTo(channel);
                }
                processed += read;
            }

            @Override
            public FilePart tryFinish() throws IOException {
                if (processed != size || name == null || nameRead != name.length) {
                    return null;
                }
                channel.force(false);
                finished = true;
                return new FilePart(target, new String(name, StandardCharsets.UTF_8));
            }

            @Override
            public void clear() {
                if (file != null) {
                    try {
                        file.close();
                    } catch (IOException ignored) {
                    }
                }
                if (!finished && target != null) {
                    try {
                        Files.deleteIfExists(target.toPath());
                    } catch (IOException ignored) {
                    }
                }
                size = 0;
                processed = 0;
                prefixRead = 0;
                nameLength = 0;
                name = null;
                nameRead = 0;
                target = null;
                file = null;
                channel = null;
                finished = false;
            }
        }
    }
}
