"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from employee import Employee
from producer import employee_topic_name

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092

    # Kafka 消费者初始化

    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = 'cdc_group'):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    # 订阅 Kafka topic 并消费数据
    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                #implement your logic here

                msg = self.poll(1.0)  # 读取 Kafka 消息
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                processing_func(msg)  # 处理消息
        except Exception as e:
            print(f"Error in consumer: {e}")


        finally:
            self.close()


# 解析 Kafka 消息并更新 ‘employees_B’ 表
def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433',      
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        #your logic goes here

        # 不同action执行不同SQL语句
        if e.action == "INSERT":
            cur.execute("""
                INSERT INTO employees_B (emp_id, first_name, last_name, dob, city)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING;  -- ✅ 防止重复插入
            """, (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city))
        
        elif e.action == "UPDATE":
            cur.execute("""
                UPDATE employees_B
                SET first_name=%s, last_name=%s, dob=%s, city=%s
                WHERE emp_id=%s;
            """, (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_id))
        
        elif e.action == "DELETE":
            cur.execute("""
                DELETE FROM employees_B WHERE emp_id=%s;
            """, (e.emp_id,))
        
        conn.commit()
        print(f"Updated employees_B: {e.to_json()}")  # 方便调试
        cur.close()
        conn.close()



    except Exception as err:
        print(f"Error updating employees_B: {err}")

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='cdc_group') 
    consumer.consume([employee_topic_name], update_dst)