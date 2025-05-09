DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS experiments;


CREATE TABLE logs AS
SELECT *
FROM read_json_auto('output/logs/*.jsonl',maximum_object_size=50000000);

CREATE TABLE experiments AS
SELECT *
FROM read_json_auto('output/experiments/*.json');

-- Select all distinct logs with level 'ERROR'
SELECT DISTINCT ex.catalog, ex.entity, l.body, l.method, ex.benchmark, ex.threads FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND l.body LIKE '%modification%'

-- Select all distinct logs with level 'ERROR' for each benchmark id
SELECT DISTINCT ex.entity, l.body, l.method, ex.benchmark, ex.threads FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND ex.benchmark = 1 AND l.body LIKE '%concurrent%';
SELECT DISTINCT ex.entity, l.body, l.method, ex.benchmark FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND ex.benchmark = 2;
SELECT DISTINCT ex.entity, l.body, l.method, ex.benchmark FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND ex.benchmark = 3;
SELECT DISTINCT ex.entity, l.body, l.method, ex.benchmark FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND ex.benchmark = 4;
SELECT DISTINCT ex.entity, l.body, l.method, ex.benchmark FROM logs l JOIN experiments ex ON l.experiment_id = ex.id WHERE l.level = 'ERROR' AND ex.benchmark = 5;


-- Count the number of errors for each benchmark and entity
SELECT ex.benchmark, ex.entity, COUNT(level) AS error_count FROM logs l JOIN experiments ex ON l.experiment_id = ex.id GROUP BY ex.benchmark,ex.entity ORDER BY error_count DESC;

