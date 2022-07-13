################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def python_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.execute_sql("""
    CREATE TABLE orders (
      order_number BIGINT,
      price        DECIMAL(32,2),
      buyer        ROW<first_name STRING, last_name STRING>,
      order_time   TIMESTAMP(3)
    ) WITH (
      'connector' = 'datagen'
    )""")

    t_env.execute_sql("""
        CREATE TABLE print_table WITH ('connector' = 'print')
          LIKE orders""")
    t_env.execute_sql("""
        INSERT INTO print_table SELECT * FROM orders""")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    python_demo()
