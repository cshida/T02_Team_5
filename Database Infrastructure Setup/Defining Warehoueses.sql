--Defining Warehouse--


CREATE OR REPLACE WAREHOUSE trustbank_etl_warehouse WITH
    WAREHOUSE_SIZE = 'xlarge'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 300  -- longer suspend time due to lengthier operations
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = False
    COMMENT = 'Used for ETL processes requiring significant computational power';

CREATE OR REPLACE WAREHOUSE trustbank_analytic_warehouse WITH
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = False
    COMMENT = 'Optimized for running complex analytic queries';

CREATE OR REPLACE WAREHOUSE trustbank_reporting_warehouse WITH
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 900  -- longer inactivity tolerance for sporadic reporting needs
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = False
    COMMENT = 'Dedicated to BI reporting, less frequent but critical queries';

CREATE OR REPLACE WAREHOUSE trustbank_loading_warehouse WITH
    WAREHOUSE_SIZE = 'xlarge'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 120  -- shorter suspend time due to frequent batch jobs
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE
    COMMENT = 'Used for bulk data loading tasks, requiring high computational resources temporarily';

CREATE OR REPLACE WAREHOUSE trustbank_dev_warehouse WITH
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60  -- quick auto-suspend as development tasks are often sporadic and short
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = False
    COMMENT = 'For development and testing, minimizing costs while providing necessary resources';

alter warehouse trustbank_analytic_warehouse resume;
