-- Setup a test database
CREATE DATABASE dbt_test
	AS PERMANENT = 60e6, -- 60MB
	    SPOOL = 120e6; -- 120MB

-- Grant permission to create stored procedures
GRANT CREATE PROCEDURE ON dbt_test TO dbc;

-- Create necessary stored procedure
replace procedure dbt_test.dbt_drop_relation_if_exists(
  IN relation_type varchar(10),
  IN full_name varchar(256)
)
begin
  DECLARE sql_stmt VARCHAR(500)  CHARACTER SET Unicode;
  DECLARE msg VARCHAR(400) CHARACTER SET Unicode;

  DECLARE exit HANDLER FOR SqlException
  BEGIN
    IF SqlCode = 3807 THEN SET msg = full_name || ' does not exist.';
    ELSEIF SqlCode = 3853 THEN SET msg = full_name || ' is not a table.';
    ELSEIF SqlCode = 3854 THEN SET msg = full_name || ' is not a view.';
    ELSE
      RESIGNAL;
    END IF;
  END;

  SET sql_stmt = 'DROP ' || relation_type || ' ' || full_name || ';';
  EXECUTE IMMEDIATE sql_stmt;
END;
