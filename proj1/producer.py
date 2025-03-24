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


from employee import Employee
import confluent_kafka
import pandas as pd
from math import floor 
import csv
import json
import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer





employee_topic_name = "employee_salaries"  # 把 bf_employee_salary 改成了 employee_salaries 因为在kafka中创建的topic名字是employee_salaries
csv_file = 'Employee_Salaries.csv'

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class DataHandler: # 已经作出修改
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def process_data(self):
        """
        - 仅保留 `ECC`、`CIT`、`EMS`
        - 仅保留 `2010` 年后的雇员
        - `Salary` 向下取整
        - 提取 `Department` 和 `Salary`
        """
        employees = [] # 用于存储符合条件的员工数据
        with open(self.csv_file, mode='r', encoding='utf-8') as file: 
            reader = csv.DictReader(file)
            
            for row in reader:
                if row["Department"] not in ["ECC", "CIT", "EMS"]: # 过滤掉不属于这三个部门的数据
                    continue  
                
                # try:
                #     hire_year = datetime.datetime.strptime(row["Initial Hire Date"], "%d-%b-%y").year
                #     if hire_year < 2000:
                #         hire_year += 100
                # except ValueError:
                #     continue  # 忽略解析失败的数据

                # if hire_year <= 2010: # 过滤掉2010年之前的员工
                #     continue  

                # 只保留2010年后的数据
                try:
                    hire_year = int(row["Initial Hire Date"][-2:]) + 2000
                except ValueError:
                    continue                

                try:
                    rounded_salary = floor(float(row["Salary"])) 
                except ValueError:
                    continue  

                # 取 Department 和 Salary
                line = [row["Department"], rounded_salary]  
                employee = Employee.from_csv_line(line)
                employees.append(employee)

        return employees



if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler(csv_file)
    producer = salaryProducer()

    for employee in reader.process_data():
        producer.produce(
            employee_topic_name,
            key=encoder(employee.emp_dept),
            value=encoder(employee.to_json())
        )
        producer.poll(1)  # 触发 Kafka 发送机制

    producer.flush()  # 确保所有数据发送成功



    '''
    # implement other instances as needed
    # you can let producer process line by line, and stop after all lines are processed, or you can keep the producer running.
    # finish code with your own logic and reasoning

    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        producer.poll(1)
    '''
    