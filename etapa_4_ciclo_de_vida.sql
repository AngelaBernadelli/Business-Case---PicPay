SQL
WITH client_month_by_month AS (
  SELECT
    client_id,
    registration_month_date,
    month_date_transaction,
    is_active_banking,

    -- Olha o mês anterior e retorna os recorrentes
    LAG(is_active_banking) OVER (
      PARTITION BY client_id
      ORDER BY month_date_transaction
    ) AS previous_month,

    -- Olha o primeiro mês quando de cliente ativo
    MIN(
      CASE 
        WHEN is_active_banking = TRUE 
        THEN month_date_transaction 
        ELSE NULL 
      END
    ) OVER (PARTITION BY client_id) AS active_first_month,
    
    -- Olha para o mês anterior ao atual
    SUM(
      CASE 
        WHEN is_active_banking = TRUE THEN 1 ELSE 0 
      END
    ) OVER (
      PARTITION BY client_id
      ORDER BY month_date_transaction
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS active_before_current,

    -- Olha para clientes com meses de entrada e transação iguais
    CASE 
      WHEN is_active_banking = TRUE
       AND registration_month_date = month_date_transaction
      THEN 'New'
      ELSE 'Old'
    END AS identify_period 
  FROM case_banking_pj.transactions_gold
),

resumo_por_mes AS (
  SELECT
    month_date_transaction,

    -- Clientes novos
    COUNT(DISTINCT CASE 
                     WHEN identify_period = 'New' 
                     THEN client_id 
                   END) AS clientes_novos,

    -- Clientes recorrentes
    COUNT(DISTINCT CASE 
                     WHEN is_active_banking = TRUE
                      AND previous_month    = TRUE
                     THEN client_id 
                   END) AS clientes_recorrentes,

    -- Clientes Ativados
    COUNT(DISTINCT CASE 
                     WHEN is_active_banking = TRUE
                      AND registration_month_date < month_date_transaction
                      AND month_date_transaction = active_first_month
                     THEN client_id
                   END) AS clientes_ativados,

    -- Clientes Reativados 
    COUNT(DISTINCT CASE 
                     WHEN is_active_banking       = TRUE
                      AND (previous_month = FALSE OR previous_month IS NULL)
                      AND active_before_current > 0
                     THEN client_id
                   END) AS clientes_reativados,

    -- Total de clientes ativos em Banking no mês 
    COUNT(DISTINCT CASE 
                     WHEN is_active_banking = TRUE 
                     THEN client_id 
                   END) AS total_clientes_ativos_mes

  FROM client_month_by_month
  GROUP BY month_date_transaction
)

-- PIVOT: linhas = categorias, colunas = meses
SELECT
  'Novos clientes' AS categoria,
  SUM(CASE WHEN month_date_transaction = 1 THEN clientes_novos             ELSE 0 END) AS mes_1,
  SUM(CASE WHEN month_date_transaction = 2 THEN clientes_novos             ELSE 0 END) AS mes_2,
  SUM(CASE WHEN month_date_transaction = 3 THEN clientes_novos             ELSE 0 END) AS mes_3
FROM resumo_por_mes

UNION ALL

SELECT
  'Recorrentes' AS categoria,
  SUM(CASE WHEN month_date_transaction = 1 THEN clientes_recorrentes       ELSE 0 END) AS mes_1,
  SUM(CASE WHEN month_date_transaction = 2 THEN clientes_recorrentes       ELSE 0 END) AS mes_2,
  SUM(CASE WHEN month_date_transaction = 3 THEN clientes_recorrentes       ELSE 0 END) AS mes_3
FROM resumo_por_mes

UNION ALL

SELECT
  'Ativados' AS categoria,
  SUM(CASE WHEN month_date_transaction = 1 THEN clientes_ativados          ELSE 0 END) AS mes_1,
  SUM(CASE WHEN month_date_transaction = 2 THEN clientes_ativados          ELSE 0 END) AS mes_2,
  SUM(CASE WHEN month_date_transaction = 3 THEN clientes_ativados          ELSE 0 END) AS mes_3
FROM resumo_por_mes

UNION ALL

SELECT
  'Reativados' AS categoria,
  SUM(CASE WHEN month_date_transaction = 1 THEN clientes_reativados        ELSE 0 END) AS mes_1,
  SUM(CASE WHEN month_date_transaction = 2 THEN clientes_reativados        ELSE 0 END) AS mes_2,
  SUM(CASE WHEN month_date_transaction = 3 THEN clientes_reativados        ELSE 0 END) AS mes_3
FROM resumo_por_mes

UNION ALL

SELECT
  'Total - Clientes ativos em Banking' AS categoria,
  SUM(CASE WHEN month_date_transaction = 1 THEN total_clientes_ativos_mes  ELSE 0 END) AS mes_1,
  SUM(CASE WHEN month_date_transaction = 2 THEN total_clientes_ativos_mes  ELSE 0 END) AS mes_2,
  SUM(CASE WHEN month_date_transaction = 3 THEN total_clientes_ativos_mes  ELSE 0 END) AS mes_3
FROM resumo_por_mes;
