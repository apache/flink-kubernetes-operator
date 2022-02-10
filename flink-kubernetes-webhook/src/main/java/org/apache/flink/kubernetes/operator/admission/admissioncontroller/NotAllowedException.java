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

package org.apache.flink.kubernetes.operator.admission.admissioncontroller;

import io.fabric8.kubernetes.api.model.Status;

/** Copied as is from https://github.com/java-operator-sdk/admission-controller-framework. */
public class NotAllowedException extends AdmissionControllerException {

    private Status status = new Status();

    public NotAllowedException() {
        status.setCode(403);
    }

    public NotAllowedException(Status status) {
        this.status = status;
    }

    public NotAllowedException(Throwable cause, Status status) {
        super(cause);
        this.status = status;
    }

    public NotAllowedException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace,
            Status status) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.status = status;
    }

    public NotAllowedException(String message) {
        super(message);
        this.status.setMessage(message);
        this.status.setCode(403);
    }

    public NotAllowedException(int code) {
        this.status.setCode(code);
    }

    public NotAllowedException(String message, int code) {
        super(message);
        this.status.setCode(code);
        this.status.setMessage(message);
    }

    public NotAllowedException(String message, Throwable cause, int code) {
        super(message, cause);
        this.status.setCode(code);
        this.status.setMessage(message);
    }

    public NotAllowedException(Throwable cause, int code) {
        super(cause);
        this.status.setCode(code);
    }

    public NotAllowedException(
            String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace,
            int code) {
        super(message, cause, enableSuppression, writableStackTrace);
        status.setCode(code);
    }

    public Status getStatus() {
        return status;
    }
}
