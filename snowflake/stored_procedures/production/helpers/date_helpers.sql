
--used for getting the mm/dd/yyyy date from the mm/dd/yy and mm-dd-yyyy viewership week (necessary for using the date_trunc function--used for getting the mm/dd/yyyy date from the mm/dd/yy and mm-dd-yyyy viewership week (necessary for using the date_trunc function)
CREATE OR REPLACE FUNCTION UPLOAD_DB_PROD.public.get_full_date(date STRING)
  RETURNS STRING
  LANGUAGE SQL
AS 
$$  
CASE
    -- When date is in yyyy-mm-dd HH:MM:SS format
    WHEN LENGTH(date) >= 19 AND POSITION('-' IN date) = 5 AND POSITION(' ' IN date) = 11 THEN
        CONCAT(
            SPLIT_PART(date, '-', 2), '/',  -- month
            SPLIT_PART(date, '-', 3), '/',  -- day
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 1)  -- year
        )
    -- When date is in mm/dd/yyyy HH:MM:SS format
    WHEN LENGTH(date) >= 19 AND POSITION('/' IN date) = 3 AND POSITION(' ' IN date) = 11 THEN
        CONCAT(
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 2), '/',  -- day
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 3)  -- year
        )
    -- When date is in mm-dd-yyyy HH:MM:SS format
    WHEN LENGTH(date) >= 19 AND POSITION('-' IN date) = 3 AND POSITION(' ' IN date) = 11 THEN
        CONCAT(
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 2), '/',  -- day
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 3)  -- year
        )
    -- When date is in mm/dd/yy HH:MM:SS format
    WHEN LENGTH(date) >= 16 AND POSITION('/' IN date) = 3 AND POSITION(' ' IN date) = 10 THEN
        CONCAT(
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 2), '/',  -- day
            CONCAT(
                CASE WHEN TO_NUMBER(SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 3)) > 50 THEN '19' ELSE '20' END,
                SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 3)  -- year
            )
        )
    -- When date is in mm-dd-yy HH:MM:SS format
    WHEN LENGTH(date) >= 16 AND POSITION('-' IN date) = 3 AND POSITION(' ' IN date) = 10 THEN
        CONCAT(
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 2), '/',  -- day
            CONCAT(
                CASE WHEN TO_NUMBER(SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 3)) > 50 THEN '19' ELSE '20' END,
                SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 3)  -- year
            )
        )
    -- When date is in yyyy-mm-dd HH:MM:SS format or yyyy-mm-dd
    WHEN POSITION('-' IN date) = 5 THEN
        CONCAT(
            SPLIT_PART(date, '-', 2), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 3), '/',  -- day
            SPLIT_PART(date, '-', 1)  -- year
        )
    -- When date is in mm/dd/yyyy HH:MM:SS format or mm/dd/yyyy
    WHEN POSITION('/' IN date) = 3 THEN
        CONCAT(
            SPLIT_PART(date, '/', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 2), '/',  -- day
            SPLIT_PART(date, '/', 3)  -- year
        )
    -- When date is in mm-dd-yyyy HH:MM:SS format or mm-dd-yyyy
    WHEN POSITION('-' IN date) = 3 THEN
        CONCAT(
            SPLIT_PART(date, '-', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 2), '/',  -- day
            SPLIT_PART(date, '-', 3)  -- year
        )
    -- When date is in mm/dd/yy HH:MM:SS format or mm/dd/yy
    WHEN LENGTH(date) >= 8 AND POSITION('/' IN date) = 3 AND LENGTH(SPLIT_PART(date, '/', 3)) = 2 THEN
        CONCAT(
            SPLIT_PART(date, '/', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '/', 2), '/',  -- day
            CONCAT(
                CASE WHEN TO_NUMBER(SPLIT_PART(date, '/', 3)) > 50 THEN '19' ELSE '20' END,
                SPLIT_PART(date, '/', 3)  -- year
            )
        )
    -- When date is in mm-dd-yy HH:MM:SS format or mm-dd-yy
    WHEN LENGTH(date) >= 8 AND POSITION('-' IN date) = 3 AND LENGTH(SPLIT_PART(date, '-', 3)) = 2 THEN
        CONCAT(
            SPLIT_PART(date, '-', 1), '/',  -- month
            SPLIT_PART(SPLIT_PART(date, ' ', 1), '-', 2), '/',  -- day
            CONCAT(
                CASE WHEN TO_NUMBER(SPLIT_PART(date, '-', 3)) > 50 THEN '19' ELSE '20' END,
                SPLIT_PART(date, '-', 3)  -- year
            )
        )
        -- When date is in mm/dd/yy format
    WHEN LENGTH(date) >= 6 AND POSITION('/' IN date) = 2 AND LENGTH(SPLIT_PART(date, '/', 3)) = 2 THEN
        CONCAT(
            LPAD(SPLIT_PART(date, '/', 1), 2, '0'), '/',  -- pad month with zero if necessary
            LPAD(SPLIT_PART(date, '/', 2), 2, '0'), '/',  -- pad day with zero if necessary
            CONCAT(
                -- Decide century based on year suffix
                CASE WHEN TO_NUMBER(SPLIT_PART(date, '/', 3)) > 50 THEN '19' ELSE '20' END,
                LPAD(SPLIT_PART(date, '/', 3), 2, '0')  -- pad year with zero if necessary
            )
        )
    ELSE
        NULL  -- Or handle invalid format as needed
END
$$;
-- GRANT USAGE ON  FUNCTION UPLOAD_DB_PROD.public.get_full_date(STRING) TO ROLE WEB_APP;

--used for getting the mm/dd/yyyy date from the mm/dd/yy viewership week (necessary for using the date_trunc function )UPLOAD_DB_PROD.public.
CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_full_date_from(date STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS 
  $$  
  CASE
      -- When date is in yyyy-mm-dd format
      WHEN POSITION('-' IN date) > 0 THEN
          CONCAT(
              SPLIT_PART(date, '-', 2), '/',  -- month
              SPLIT_PART(date, '-', 3), '/',  -- day
              SPLIT_PART(date, '-', 1)        -- year
          )
      -- When date is in mm/dd/yyyy format
      WHEN LENGTH(date) = 10 AND POSITION('/' IN date) > 0 THEN
          date
      -- Else, assume it's in mm/dd/yy and convert to mm/dd/yyyy
      ELSE
          CONCAT(
              SPLIT_PART(date, '/', 1), '/',
              SPLIT_PART(date, '/', 2), '/',
              CONCAT('20', SPLIT_PART(date, '/', 3))
          )
  END
  $$;


--used for getting the date for the Monday of the viewership week
CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_week_start(date_string STRING)
  RETURNS DATE
  LANGUAGE SQL
  AS 
  $$  
  DATE_TRUNC('WEEK', TO_DATE(date_string, 'MM/DD/YYYY'))
  $$;


--gets quarter based on month value in date column 
CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_quarter_from_mm_dd_yyyy(date_string STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS 
  $$  
  CASE
      WHEN EXTRACT(MONTH, TO_DATE(date_string, 'MM/DD/YYYY')) BETWEEN 1 AND 3 THEN 'q1'
      WHEN EXTRACT(MONTH, TO_DATE(date_string, 'MM/DD/YYYY')) BETWEEN 4 AND 6 THEN 'q2'
      WHEN EXTRACT(MONTH, TO_DATE(date_string, 'MM/DD/YYYY')) BETWEEN 7 AND 9 THEN 'q3'
      ELSE 'q4'
  END
  $$;


--gets year based on date value in date column 
  CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_year_from_mm_dd_yyyy(date_string STRING)
  RETURNS NUMBER
  LANGUAGE SQL
  AS 
  $$  
  EXTRACT(YEAR, TO_DATE(date_string, 'MM/DD/YYYY'))
  $$;


--gets month based on date value in date column 
  CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_month_from_mm_dd_yyyy(date_string STRING)
  RETURNS NUMBER
  LANGUAGE SQL
  AS 
  $$  
  EXTRACT(MONTH, TO_DATE(date_string, 'MM/DD/YYYY'))
  $$;



--get first of month aka year_month_day based on date value in date column 
  CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_first_of_month_from_mm_dd_yyyy(date_string STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS 
  $$  
    TO_VARCHAR(TO_DATE(date_string, 'MM/DD/YYYY'), 'YYYYMM') || '01'
  $$;




--gets day based on date value in date column 
  CREATE OR REPLACE FUNCTION  UPLOAD_DB_PROD.public.get_day_from_mm_dd_yyyy(date_string STRING)
  RETURNS NUMBER
  LANGUAGE SQL
  AS 
  $$  
  EXTRACT(DAY, TO_DATE(date_string, 'MM/DD/YYYY'))
  $$;

