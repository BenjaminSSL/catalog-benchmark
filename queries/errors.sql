DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS experiments;

CREATE TABLE logs AS
SELECT *
FROM read_json_auto('output/logs/*.jsonl');

CREATE TABLE experiments AS
SELECT *
FROM read_json_auto('output/experiments/*.json');


SELECT logs.experiment_id, COUNT(level) AS error_count FROM logs GROUP BY logs.experiment_id ORDER BY error_count DESC;

SELECT * FROM LOGS JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.type LIKE '%listCatalog%';

SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.body LIKE '%sibling%'


-- CREATE ERRORS
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createCatalog%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createPrincipal%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark, experiments.threads FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createSchema%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createTable%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createView%';

-- DELETE ERRORS
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%deleteCatalog%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%deletePrincipal%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%deleteSchema%' AND logs.body NOT LIKE '%does not exist%' AND logs.body NOT LIKE '%dial tcp%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%deleteTable%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%deleteView%';

-- GET ERRORS
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%getCatalog%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%getPrincipal%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%getSchema%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%getTable%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%getView%';


-- LIST ERRORS
SELECT DISTINCT logs.body, logs.type, experiments.benchmark, experiments.threads FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%listCatalog%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark, experiments.threads FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%listPrincipal%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%listSchema%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%listTable%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%listView%';


-- UPDATE ERRORS
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%updateCatalog%' AND logs.body NOT LIKE '%currentEntityVersion%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%updatePrincipal%' AND logs.body NOT LIKE '%currentEntityVersion%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark, experiments.threads FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%updateSchema%' AND logs.body NOT LIKE '%currentEntityVersion%' AND logs.status_code = 500;
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%updateTable%' AND logs.body NOT LIKE '%currentEntityVersion%' AND logs.body LIKE '%retry%';
SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%updateView%' AND logs.body NOT LIKE '%currentEntityVersion%';


SELECT DISTINCT logs.body, logs.type, experiments.benchmark FROM logs JOIN experiments ON logs.experiment_id = experiments.id WHERE logs.level = 'ERROR' AND logs.type LIKE '%createTable%' AND logs.body NOT LIKE '%currentEntityVersion%';