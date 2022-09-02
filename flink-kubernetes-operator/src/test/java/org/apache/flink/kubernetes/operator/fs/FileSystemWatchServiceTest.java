/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.fs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Test for filesystem watch service. */
class FileSystemWatchServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemWatchServiceTest.class);

    private @TempDir Path tmpDir;
    private String fileName;
    private Path fileFullPath;
    private CountDownLatch watchStartedLatch;
    private CountDownLatch watchEventArrivedLatch;
    private FileSystemWatchService fileSystemWatchService;

    @BeforeEach
    public void beforeEach() {
        fileName = UUID.randomUUID().toString();
        fileFullPath = Paths.get(tmpDir.toString(), fileName);
        watchStartedLatch = new CountDownLatch(1);
        watchEventArrivedLatch = new CountDownLatch(1);
        fileSystemWatchService = null;
    }

    @AfterEach
    public void afterEach() throws InterruptedException {
        if (fileSystemWatchService != null) {
            fileSystemWatchService.interrupt();
            fileSystemWatchService.join(10_000);
            fileSystemWatchService = null;
        }
    }

    @Test
    public void testMissingDirectory() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> {
                    new FileSystemWatchService("/intentionally/missing/directory");
                });
    }

    @Test
    public void testFileCreateEvent() throws Exception {
        FileSystemWatchService fileSystemWatchService =
                new FileSystemWatchService(tmpDir.toString()) {
                    @Override
                    protected void onWatchStarted(Path realDirectoryPath) {
                        watchStartedLatch.countDown();
                    }

                    @Override
                    protected void onFileOrDirectoryCreated(Path relativePath) {
                        if (relativePath.toString().equals(fileName)) {
                            watchEventArrivedLatch.countDown();
                        }
                    }
                };
        fileSystemWatchService.start();
        Assertions.assertTrue(watchStartedLatch.await(10, TimeUnit.SECONDS));

        Files.createFile(Paths.get(tmpDir.toString(), fileName));

        Assertions.assertTrue(watchEventArrivedLatch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testFileDeleteEvent() throws Exception {
        Files.createFile(fileFullPath);
        Assertions.assertEquals(1, Files.list(tmpDir).count());
        FileSystemWatchService fileSystemWatchService =
                new FileSystemWatchService(tmpDir.toString()) {
                    @Override
                    protected void onWatchStarted(Path realDirectoryPath) {
                        LOG.info("onWatchStarted");
                        watchStartedLatch.countDown();
                    }

                    @Override
                    protected void onFileOrDirectoryDeleted(Path relativePath) {
                        LOG.info("onFileOrDirectoryDeleted");
                        if (relativePath.toString().equals(fileName)) {
                            watchEventArrivedLatch.countDown();
                        }
                    }
                };
        fileSystemWatchService.start();
        Assertions.assertTrue(watchStartedLatch.await(10, TimeUnit.SECONDS));

        Files.delete(fileFullPath);

        Assertions.assertEquals(0, Files.list(tmpDir).count());
        Assertions.assertTrue(watchEventArrivedLatch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void testFileModifyEvent() throws Exception {
        writeFile("1");

        FileSystemWatchService fileSystemWatchService =
                new FileSystemWatchService(tmpDir.toString()) {
                    @Override
                    protected void onWatchStarted(Path realDirectoryPath) {
                        watchStartedLatch.countDown();
                    }

                    @Override
                    protected void onFileOrDirectoryModified(Path relativePath) {
                        if (relativePath.toString().equals(fileName)) {
                            watchEventArrivedLatch.countDown();
                        }
                    }
                };
        fileSystemWatchService.start();
        Assertions.assertTrue(watchStartedLatch.await(10, TimeUnit.SECONDS));

        writeFile("2");

        Assertions.assertTrue(watchEventArrivedLatch.await(1, TimeUnit.MINUTES));
    }

    private void writeFile(String content) throws IOException {
        Files.write(
                fileFullPath,
                content.getBytes(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
