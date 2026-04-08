-- ============================================================
-- HR DATABASE SCHEMA
-- Works on both MySQL 8+ and PostgreSQL 14+
-- Run this once to set up the database
-- ============================================================

-- ── 1. EMPLOYEES (core table — mirrors your original CSV) ──
CREATE TABLE IF NOT EXISTS employees (
    employee_id     VARCHAR(20)     PRIMARY KEY,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    gender          VARCHAR(10),
    birthdate       DATE,
    hiredate        DATE            NOT NULL,
    termdate        DATE            DEFAULT NULL,   -- NULL = currently active
    department      VARCHAR(100),
    job_title       VARCHAR(150),
    state           VARCHAR(100),
    city            VARCHAR(100),
    education_level VARCHAR(100),
    salary          DECIMAL(12, 2),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    -- PostgreSQL: use  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  and add trigger below
);

-- ── 2. PERFORMANCE RATINGS (time-series — one row per review) ──
CREATE TABLE IF NOT EXISTS performance_ratings (
    id              INT             AUTO_INCREMENT PRIMARY KEY,  -- PostgreSQL: SERIAL PRIMARY KEY
    employee_id     VARCHAR(20)     NOT NULL,
    review_date     DATE            NOT NULL,
    performance     VARCHAR(50)     NOT NULL,   -- e.g. 'Excellent', 'Good', 'Needs Improvement'
    score           DECIMAL(4, 2),              -- optional numeric score 0–5
    reviewer        VARCHAR(100),
    notes           TEXT,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id) ON DELETE CASCADE
);

-- ── 3. SALARY BENCHMARKS (populated by the Python salary script) ──
CREATE TABLE IF NOT EXISTS salary_benchmarks (
    id              INT             AUTO_INCREMENT PRIMARY KEY,
    job_title       VARCHAR(150)    NOT NULL,
    state           VARCHAR(100),
    benchmark_low   DECIMAL(12, 2),
    benchmark_mid   DECIMAL(12, 2),
    benchmark_high  DECIMAL(12, 2),
    source          VARCHAR(50)     DEFAULT 'BLS',
    as_of_date      DATE            NOT NULL,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- ── 4. ATTRITION RISK SCORES (written by ML pipeline) ──
CREATE TABLE IF NOT EXISTS attrition_risk (
    employee_id     VARCHAR(20)     PRIMARY KEY,
    risk_score      DECIMAL(5, 4)   NOT NULL,     -- 0.0000 to 1.0000
    risk_label      VARCHAR(20)     NOT NULL,      -- 'Low', 'Medium', 'High'
    top_factor_1    VARCHAR(100),
    top_factor_2    VARCHAR(100),
    top_factor_3    VARCHAR(100),
    model_version   VARCHAR(20),
    scored_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id) ON DELETE CASCADE
);

-- ── INDEXES for query performance ──
CREATE INDEX IF NOT EXISTS idx_emp_dept      ON employees(department);
CREATE INDEX IF NOT EXISTS idx_emp_state     ON employees(state);
CREATE INDEX IF NOT EXISTS idx_emp_status    ON employees(termdate);
CREATE INDEX IF NOT EXISTS idx_perf_emp      ON performance_ratings(employee_id);
CREATE INDEX IF NOT EXISTS idx_perf_date     ON performance_ratings(review_date);
CREATE INDEX IF NOT EXISTS idx_bench_title   ON salary_benchmarks(job_title);
CREATE INDEX IF NOT EXISTS idx_risk_score    ON attrition_risk(risk_score);

-- ============================================================
-- POSTGRESQL ONLY: auto-update updated_at trigger
-- (Skip this block if using MySQL)
-- ============================================================
/*
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_employees_updated
BEFORE UPDATE ON employees
FOR EACH ROW EXECUTE FUNCTION update_updated_at();
*/

-- ============================================================
-- SAMPLE DATA INSERT (small test batch — remove in production)
-- ============================================================
INSERT IGNORE INTO employees (employee_id, first_name, last_name, gender, birthdate,
    hiredate, termdate, department, job_title, state, city, education_level, salary)
VALUES
('E001','Alice','Johnson','Female','1990-03-15','2018-06-01',NULL,'Engineering','Software Engineer','New York','New York City','Bachelor',95000),
('E002','Bob','Smith','Male','1985-07-22','2015-03-10',NULL,'HR','HR Manager','Texas','Austin','Master',85000),
('E003','Carol','Lee','Female','1992-11-30','2020-01-15','2023-08-01','Finance','Financial Analyst','California','San Francisco','Bachelor',78000),
('E004','David','Brown','Male','1988-05-10','2017-09-01',NULL,'Engineering','Senior Engineer','New York','New York City','Master',120000),
('E005','Eva','Davis','Female','1995-02-14','2022-04-01',NULL,'Marketing','Marketing Analyst','Texas','Houston','Bachelor',65000);

-- Sample performance ratings (multiple reviews per employee = time-series)
INSERT IGNORE INTO performance_ratings (employee_id, review_date, performance, score)
VALUES
('E001','2021-12-31','Good',3.5),
('E001','2022-12-31','Excellent',4.8),
('E001','2023-12-31','Excellent',4.9),
('E002','2021-12-31','Good',3.2),
('E002','2022-12-31','Good',3.5),
('E002','2023-12-31','Excellent',4.2),
('E004','2022-12-31','Excellent',5.0),
('E004','2023-12-31','Excellent',4.7);
