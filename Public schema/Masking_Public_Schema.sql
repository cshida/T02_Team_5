use role sysadmin;
use database trustbankdata;
use warehouse trustbank_dev_warehouse;

CREATE OR REPLACE MASKING POLICY address_masking_policy AS
  (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('FULL_ACCESS_ROLE') THEN val  -- Full access roles see the full address
      ELSE CONCAT(SUBSTRING(val, 1, POSITION(',' IN val)), 'XXXX, Singapore')  -- Mask part of the address
    END;


ALTER TABLE TRUSTBANKDATA.PUBLIC.CUSTOMER_DEMO
MODIFY COLUMN ADDRESS SET MASKING POLICY address_masking_policy;

CREATE OR REPLACE MASKING POLICY cardholder_id_masking_policy AS
  (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('AUTHORIZED_ROLE') THEN val  -- Only authorized roles can see the full ID
      ELSE CONCAT('XXXXXX', RIGHT(val, 4))  -- Mask the ID except for the last 4 digits
    END;

-- Apply to CUSTOMER_DEMO
ALTER TABLE TRUSTBANKDATA.PUBLIC.CUSTOMER_DEMO
MODIFY COLUMN CardholderID SET MASKING POLICY cardholder_id_masking_policy;


-- Apply to CUSTOMER_LOOKUP
ALTER TABLE TRUSTBANKDATA.PUBLIC.CUSTOMER_LOOKUP
MODIFY COLUMN CardholderID SET MASKING POLICY cardholder_id_masking_policy;

-- Apply to INVESTMENTS
ALTER TABLE TRUSTBANKDATA.PUBLIC.INVESTMENTS
MODIFY COLUMN CardholderID SET MASKING POLICY cardholder_id_masking_policy;

ALTER TABLE TRUSTBANKDATA.PUBLIC.INVESTMENTS
MODIFY COLUMN INVESTMENTID SET MASKING POLICY cardholder_id_masking_policy;

-- Apply to LOAN
ALTER TABLE TRUSTBANKDATA.PUBLIC.LOAN
MODIFY COLUMN CardholderID SET MASKING POLICY cardholder_id_masking_policy;

ALTER TABLE TRUSTBANKDATA.PUBLIC.LOAN
MODIFY COLUMN LOANID SET MASKING POLICY cardholder_id_masking_policy;

-- Apply to TRANSACTIONS
ALTER TABLE TRUSTBANKDATA.PUBLIC.TRANSACTIONS
MODIFY COLUMN CardholderID SET MASKING POLICY cardholder_id_masking_policy;


