sql
-- Contagem de Clientes ativos em Banking em cada mês
SELECT
  month_date_transaction,
  COUNT(DISTINCT client_id) AS clientes_ativos_mes
FROM case_banking_pj.transactions_gold
WHERE is_active_banking = TRUE
GROUP BY month_date_transaction
ORDER BY month_date_transaction;
