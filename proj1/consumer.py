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
from producer import employee_topic_name #you do not want to hard copy it


class SalaryConsumer(Consumer):
    # 作用是从kafka中消费数据 也就是订阅‘employee_topic_name’主题，然后将数据插入到postgres数据库中
    
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = 'salary_consumer_group'):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        #  作用是订阅 Kafka 主题并处理消息

        #implement your message processing logic here. Not necessary to follow the template. 
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)


                #can implement other logics for msg
                if msg is None:
                    continue
                if msg.error():
                    print(f"Kafka 消费错误: {msg.error()}")
                    continue
                processing_func(msg)
        finally:
            self.close()


#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_salary(msg):
        # 作用是从Kafka读取数据，并更新 PostgreSQL 中的‘department_employee_salary’ 表
        
        e = Employee(**(json.loads(msg.value())))
        print(f"Processing Kafka message: {e.to_json()}")  # 增添一下打印日志的效果方便检查

        try:
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                host="localhost", # 我在主机运行，更改为localhost
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic goes here


            # 通过SQL更新‘department_employee_salary’表
            sql = """
                INSERT INTO department_employee_salary (department, total_salary)
                VALUES (%s, %s)
                ON CONFLICT (department) DO UPDATE
                SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
            """
            cur.execute(sql, (e.emp_dept, e.emp_salary))

            print(f"Updated salary for department: {e.emp_dept}")  # 打印更新时候的日志
            cur.close()
            conn.close() # 关闭连接

        except Exception as err:
            print(f"Database error: {err}")

if __name__ == '__main__':
    consumer = SalaryConsumer(group_id="salary_consumer_group")  # group_id为‘salary_consumer_group‘
    consumer.consume([employee_topic_name], ConsumingMethods.add_salary)  # 订阅‘employee_topic_name’
