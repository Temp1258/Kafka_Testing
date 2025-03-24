DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS emp_cdc;
DROP TABLE IF EXISTS employees_B;

-- 先删除已有的触发器
DROP TRIGGER IF EXISTS emp_changes ON employees;

-- 创建 employees 表（源表）
CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100)
);

-- 创建 emp_cdc 表（用于存储变更数据）
CREATE TABLE emp_cdc (
    action_id SERIAL PRIMARY KEY,
    emp_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    action VARCHAR(100)
);

-- 创建 employees_B（目标表，用于同步 Kafka 发送的数据）
CREATE TABLE employees_B (
    emp_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    CONSTRAINT unique_emp_id UNIQUE (emp_id) -- 确保 emp_id 唯一，防止重复插入
);


-- 创建触发器函数 capture_changes()，它会在 employees 表有变更时，把变化记录到 emp_cdc 表里。
CREATE OR REPLACE FUNCTION capture_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'INSERT');
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'UPDATE');
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO emp_cdc (emp_id, first_name, last_name, dob, city, action)
        VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, 'DELETE');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;




-- 创建触发器 emp_changes  如果运行成功，employees表则已经能自动记录变更到emp_cdc表
CREATE TRIGGER emp_changes
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION capture_changes();









-- 测试触发器（手动插入一条数据到 employees，看看 emp_cdc 表是否有记录）
INSERT INTO employees (first_name, last_name, dob, city) 
VALUES ('Alice', 'Smith', '1985-06-15', 'Los Angeles');

INSERT INTO employees (first_name, last_name, dob, city) 
VALUES 
('Max', 'Smith', '2002-02-03', 'Sydney'),
('Karl', 'Summers', '2004-04-10', 'Brisbane'),
('Sam', 'Wilde', '2005-02-06', 'Perth');

INSERT INTO employees (first_name, last_name, dob, city) 
VALUES ('John', 'Doe', '1990-01-01', 'New York');

UPDATE employees SET city = 'San Francisco' WHERE first_name = 'John';

DELETE FROM employees WHERE first_name = 'John';





SELECT * FROM emp_cdc;
SELECT * FROM employees_B;  -- 在跑完producer.py和consumer.py之后会被kafka监听到并插入到这个表里
SELECT * FROM employees;







