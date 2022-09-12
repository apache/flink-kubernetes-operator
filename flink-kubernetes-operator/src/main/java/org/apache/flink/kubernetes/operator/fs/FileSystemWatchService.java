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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/** Service which is able to watch local filesystem directories. */
public class FileSystemWatchService extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemWatchService.class);

    private final String directoryPath;

    public FileSystemWatchService(String directoryPath) {
        if (!new File(directoryPath).isDirectory()) {
            throw new IllegalArgumentException("Directory must exists: " + directoryPath);
        }
        this.directoryPath = directoryPath;
    }

    @Override
    public void run() {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            LOG.info("Starting watching path: " + directoryPath);
            Path realDirectoryPath = Paths.get(directoryPath).toRealPath();
            LOG.info("Path is resolved to real path: " + realDirectoryPath);
            realDirectoryPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
            onWatchStarted(realDirectoryPath);

            while (true) {
                LOG.debug("Taking watch key");
                WatchKey watchKey = watcher.take();
                LOG.debug("Watch key arrived");
                for (WatchEvent<?> watchEvent : watchKey.pollEvents()) {
                    LOG.debug("Watch event count: " + watchEvent.count());
                    if (watchEvent.kind() == OVERFLOW) {
                        LOG.error("Filesystem events may have been lost or discarded");
                        Thread.yield();
                    } else if (watchEvent.kind() == ENTRY_CREATE) {
                        onFileOrDirectoryCreated((Path) watchEvent.context());
                    } else if (watchEvent.kind() == ENTRY_DELETE) {
                        onFileOrDirectoryDeleted((Path) watchEvent.context());
                    } else if (watchEvent.kind() == ENTRY_MODIFY) {
                        onFileOrDirectoryModified((Path) watchEvent.context());
                    } else {
                        throw new IllegalStateException("Invalid event kind: " + watchEvent.kind());
                    }
                }
                watchKey.reset();
            }
        } catch (InterruptedException e) {
            LOG.info("Filesystem watcher interrupted");
        } catch (Exception e) {
            LOG.error("Filesystem watcher received exception and stopped: " + e);
            throw new RuntimeException(e);
        }
    }

    protected void onWatchStarted(Path realDirectoryPath) {}

    protected void onFileOrDirectoryCreated(Path relativePath) {}

    protected void onFileOrDirectoryDeleted(Path relativePath) {}

    protected void onFileOrDirectoryModified(Path relativePath) {}
}
