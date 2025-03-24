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

import csv
import json
import os
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
from confluent_kafka.serialization import StringSerializer
import psycopg2
import time



employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True

    # 从 emp_cdc 表获取数据并发送到 Kafka
    def fetch_cdc(self,):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5433', 
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here
            

            # 查询 emp_cdc 表中的数据
            cur.execute("SELECT * FROM emp_cdc;")
            rows = cur.fetchall()

            # 遍历查询结果
            for row in rows:
                emp = Employee.from_line(row)  # 转换成 Employee 对象
                message = emp.to_json()  

                # 发送数据到 Kafka
                self.produce(employee_topic_name, message)
                print(f"Produced: {message}")  # 打印方便调试

            # 处理完数据后，删除 emp_cdc 表中的数据，防止重复消费
            cur.execute("DELETE FROM emp_cdc;")
            conn.commit()







            cur.close()
        except Exception as err:
            print(f"Error fetching CDC: {err}")

    

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    
    while producer.running:
        producer.fetch_cdc()  # 每次循环从数据库获取数据
        time.sleep(5)  # 防止占用CPU过高
    
