SELECT scrIds, lower(ProviderName_3) as ProviderName_3, lower(CurrencyCode_3) as CurrencyCode_3, cast(Amount_3 as decimal(38,5)) as Amount_3
 FROM recon_test.source_data_11;
 
SELECT scrIds, lower(Provider_4) as Provider_4, lower(Currency_4) as Currency_4, cast(Amt_4 as decimal(38,5)) as Amt_4
FROM recon_test.bank_data_11;

SELECT * FROM recon_test.t_reconciliation_result 
WHERE target_view_id = 4;

SELECT s.* FROM recon_test.source_data_11 s 
WHERE s.scrIds NOT in (SELECT t.original_row_id as scrIds From recon_test.t_reconciliation_result t
                       WHERE original_view_id = 3)
;

SELECT t.* FROM recon_test.bank_data_11 t 
WHERE t.scrIds NOT in (SELECT t.original_row_id as scrIds From recon_test.t_reconciliation_result t
                       WHERE target_view_id = 4)
;
-- ONE TO ONE : Source
SELECT s1.scrIds, lower(s1.ProviderName_3) as ProviderName_3, lower(s1.CurrencyCode_3) as CurrencyCode_3, cast(s1.Amount_3 as decimal(38,5)) as Amount_3
 FROM   recon_test.source_data_11 s1 Join (
SELECT  max(src.scrIds) as scrIds FROM 
(SELECT s.scrIds, lower(s.ProviderName_3) as ProviderName_3, lower(s.CurrencyCode_3) as CurrencyCode_3, cast(s.Amount_3 as decimal(38,5)) as Amount_3
 FROM   recon_test.source_data_11 s 
 WHERE  s.scrIds NOT in (SELECT t.original_row_id as scrIds From recon_test.t_reconciliation_result t
                         WHERE original_view_id = 3) ) src
GROUP BY src.ProviderName_3, src.CurrencyCode_3, src.Amount_3
HAVING count(1) = 1 ) uniquesource on 
s1.scrIds = uniquesource.scrIds
;

-- ONE TO ONE : Target
SELECT t1.scrIds, lower(t1.Provider_4) as Provider_4, lower(t1.Currency_4) as Currency_4, cast(t1.Amt_4 as decimal(38,5)) as Amt_4
FROM recon_test.bank_data_11 t1 Join 
(SELECT  max(tar.scrIds) as scrIds FROM 
(SELECT t.scrIds, lower(t.Provider_4) as Provider_4, lower(t.Currency_4) as Currency_4, cast(t.Amt_4 as decimal(38,5)) as Amt_4
 FROM   recon_test.bank_data_11 t 
 WHERE  t.scrIds NOT in (SELECT x.target_row_id as scrIds From recon_test.t_reconciliation_result x
                         WHERE x.target_view_id = 4) ) tar
GROUP BY tar.Provider_4, tar.Currency_4, tar.Amt_4
HAVING count(1) = 1 ) uniquetarget on
t1.scrIds = uniquetarget.scrids 
;

-- ONE TO ONE join

SELECT s2.*, t2.* FROM 
(SELECT s1.scrIds, lower(s1.ProviderName_3) as ProviderName_3, lower(s1.CurrencyCode_3) as CurrencyCode_3, cast(s1.Amount_3 as decimal(38,5)) as Amount_3
 FROM   recon_test.source_data_11 s1 Join (
SELECT  max(src.scrIds) as scrIds FROM 
(SELECT s.scrIds, lower(s.ProviderName_3) as ProviderName_3, lower(s.CurrencyCode_3) as CurrencyCode_3, cast(s.Amount_3 as decimal(38,5)) as Amount_3
 FROM   recon_test.source_data_11 s 
 WHERE  s.scrIds NOT in (SELECT t.original_row_id as scrIds From recon_test.t_reconciliation_result t
                         WHERE original_view_id = 3) ) src
GROUP BY src.ProviderName_3, src.CurrencyCode_3, src.Amount_3
HAVING count(1) = 1 ) uniquesource on 
s1.scrIds = uniquesource.scrIds) s2,
 
(SELECT t1.scrIds, lower(t1.Provider_4) as Provider_4, lower(t1.Currency_4) as Currency_4, cast(t1.Amt_4 as decimal(38,5)) as Amt_4
FROM recon_test.bank_data_11 t1 Join 
(SELECT  max(tar.scrIds) as scrIds FROM 
(SELECT t.scrIds, lower(t.Provider_4) as Provider_4, lower(t.Currency_4) as Currency_4, cast(t.Amt_4 as decimal(38,5)) as Amt_4
 FROM   recon_test.bank_data_11 t 
 WHERE  t.scrIds NOT in (SELECT x.target_row_id as scrIds From recon_test.t_reconciliation_result x
                         WHERE x.target_view_id = 4) ) tar
GROUP BY tar.Provider_4, tar.Currency_4, tar.Amt_4
HAVING count(1) = 1 ) uniquetarget on
t1.scrIds = uniquetarget.scrids ) t2

/*WHERE s2.ProviderName_3 = t2.Provider_4 
AND  s2.CurrencyCode_3 = t2.Currency_4 
AND  t2.Amt_4 BETWEEN s2.Amount_3 -0.44 AND s2.Amount_3 +0.44  */

 WHERE s2.ProviderName_3 = t2.Provider_4 
  AND s2.CurrencyCode_3 = t2.Currency_4 
  AND s2.Amount_3 = t2.Amt_4
;

DESCRIBE recon_test.t_reconciliation_result ;

SELECT s.id,s.original_row_id, t.target_row_id, s.recon_reference,t.recon_reference 
FROM recon_test.t_reconciliation_result s, recon_test.t_reconciliation_result t
WHERE s.recon_reference = t.recon_reference
AND s.original_row_id IS NOT NULL
AND t.target_row_id IS NOT NULL
AND s.reconciliation_user_id LIKE "999999%";

SELECT DISTINCT reconciliation_rule_id  FROM recon_test.t_reconciliation_result;
SELECT DISTINCT reconciled_date FROM recon_test.t_reconciliation_result;
SELECT max(recon_reference) FROM recon_test.t_reconciliation_result
WHERE reconciliation_rule_id = 11
AND target_row_id IS NOT NULL;

DELETE FROM recon_test.t_reconciliation_result 
WHERE reconciliation_user_id LIKE "999999%";

SELECT reconciliation_rule_id, max(recon_reference), count(1) FROM recon_test.t_reconciliation_result 
WHERE reconciliation_user_id LIKE "999999%"
GROUP BY 1;

TRUNCATE recon_test.t_reconciliation_result;

COMMIT;

SELECT b.id FROM recon_test.t_reconciliation_result b
GROUP BY b.id
HAVING COUNT(1) > 1;

select id, info from information_schema.processlist where info is not NULL and info not like '%information_schema%';


SELECT s.* FROM recon_test.source_data_11 s
where s.CurrencyCode_3 = "usa";
