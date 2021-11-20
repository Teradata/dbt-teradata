
replace procedure dbt_test.dbt_drop_view_if_exists(
  IN full_name varchar(256)
)
begin
  DECLARE sql_stmt VARCHAR(500)  CHARACTER SET Unicode;
  DECLARE msg VARCHAR(400) CHARACTER SET Unicode;

  DECLARE exit HANDLER FOR SqlException
  BEGIN
    IF SqlCode = 3807 THEN SET msg = full_name || ' does not exist.';
    ELSEIF SqlCode = 3854 THEN SET msg = full_name || ' is not a view.';
    ELSE
      RESIGNAL;
    END IF;
  END;

  SET sql_stmt = 'DROP VIEW ' || full_name || ';';
  EXECUTE IMMEDIATE sql_stmt;
END;
