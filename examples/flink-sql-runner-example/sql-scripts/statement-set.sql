/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE orders (
  order_number BIGINT,
  price        DECIMAL(32,2),
  buyer        ROW<first_name STRING, last_name STRING>,
  order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen'
);

CREATE TABLE print_table WITH ('connector' = 'print')
    LIKE orders;
CREATE TABLE blackhole_table WITH ('connector' = 'blackhole')
    LIKE orders;

EXECUTE STATEMENT SET
BEGIN
INSERT INTO print_table SELECT * FROM orders;
INSERT INTO blackhole_table SELECT * FROM orders;
END;
