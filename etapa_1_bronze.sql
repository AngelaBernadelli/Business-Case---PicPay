```sql
-- 1. Criação da tabela transacionais_pj dentro do schema case_banking_pj para trazer o .csv da forma como ele está no arquivo 
CREATE TABLE case_banking_pj.transactions_bronze (
  client_id TEXT,
  month_date_transaction TEXT,
  registration_month_date TEXT,
  product_type TEXT,
  direction TEXT,
  client_action TEXT,
  is_approved TEXT,
  total_value TEXT
);

-- 2. Trazer csv para tabela "Table Import Wizard" > C:\Users\angela.bernadelli\Desktop\Pessoal\Case - Analytics - Banking PJ - v2\transactions_v5.csv > testar com SELECT 
SELECT * FROM case_banking_pj.transactions_bronze;

-- 3. Verificar registros distintos na coluna product_type para selecionar "Somente transações de Banking PJ" e "Somente transações ativas" 
SELECT DISTINCT product_type FROM case_banking_pj.transactions_bronze ORDER BY 1;

-- 4. Transformar o tipo das colunas e incluir a coluna "is_activity_transaction_banking"
-- DROP TABLE IF EXISTS case_banking_pj.transactions_silver;

CREATE TABLE case_banking_pj.transactions_silver (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
    COMMENT 'Identificador único da transação (PK)',

  client_id INT UNSIGNED NOT NULL
    COMMENT 'Identificador único do cliente (PJ)',

  month_date_transaction INT NOT NULL
    COMMENT 'Mês em que a transação aconteceu (1,2,3)',

  registration_month_date INT NOT NULL
    COMMENT 'Mês em que o cliente criou a conta (1,2,3)',

  product_type VARCHAR(100) NOT NULL
    COMMENT 'Qual o produto utilizado na transação',

  direction CHAR(3) NOT NULL
    COMMENT 'IN = recurso entrando | OUT = recurso saindo da conta',

  client_action BOOLEAN NOT NULL
    COMMENT 'TRUE = ativa (ação do cliente) | FALSE = passiva (movimento automático)',

  is_approved BOOLEAN NOT NULL
    COMMENT 'TRUE = transação aprovada | FALSE = transação com falha',

  total_value DECIMAL(18,2) NOT NULL
    COMMENT 'Valor movimentado pela transação (em reais)',

  is_activity_transaction_banking BOOLEAN NOT NULL
    COMMENT 'TRUE = transação válida para Banking PJ ativo | FALSE = demais casos'
)
COMMENT='Tabela Silver: transações de Banking PJ padronizadas e tratadas';

-- 5. Popular a ultima coluna "Somente transações de Banking PJ" e "Somente transações ativas"
INSERT INTO case_banking_pj.transactions_silver (
  client_id,
  month_date_transaction,
  registration_month_date,
  product_type,
  direction,
  client_action,
  is_approved,
  total_value,
  is_activity_transaction_banking
)
SELECT
  CAST(TRIM(b.client_id) AS UNSIGNED)                            				          AS client_id,
  CAST(TRIM(b.month_date_transaction) AS SIGNED)                 				          AS month_date_transaction,
  CAST(TRIM(b.registration_month_date) AS SIGNED)                				          AS registration_month_date,
  TRIM(b.product_type)                                           				          AS product_type,
  UPPER(TRIM(b.direction))                                       				          AS direction,
  (LOWER(TRIM(b.client_action))  = 'true')                       				          AS client_action,
  (LOWER(TRIM(b.is_approved))    = 'true')                       				          AS is_approved,
  CAST(REPLACE(REPLACE(TRIM(b.total_value), ' ', ''), ',', '.') AS DECIMAL(18,2)) AS total_value,

  -- Banking PJ: ativa + aprovada - aceitação/passivo 
  (
    (LOWER(TRIM(b.is_approved)) = 'true')
    AND (LOWER(TRIM(b.client_action)) = 'true')
    AND (
         b.product_type LIKE 'CARTAO_%'           -- cartão como pagador
      OR b.product_type LIKE '%APORTE_MANUAL%'    -- cofrinho aporte manual
      OR b.product_type LIKE 'PIX_%'              -- pix como pagador 
      OR b.product_type LIKE 'EMISSAO_BOLETO%'    -- emissão boleto 
      OR b.product_type LIKE 'PAGAMENTO_BOLETO%'  -- pagamento de boleto
    )
    AND (
         b.product_type NOT LIKE '%LINK%'         -- link de pagamento (aceitação)
      AND b.product_type NOT LIKE '%RECEIVEMENT%' -- pix recebido por chave (passivo)
      AND b.product_type NOT LIKE '%AUTO%'        -- automáticos (passivo)
      AND b.product_type NOT LIKE '%POS%'         -- maquininha (aceitação)
    )
  )  																			  AS is_activity_transaction_banking
  
FROM case_banking_pj.transactions_bronze b;

-- 6. Verificação da tabela Silver
SELECT * FROM case_banking_pj.transactions_silver;

-- 7. Contagem de linhas carregadas
SELECT COUNT(*) AS linhas_silver FROM case_banking_pj.transactions_silver;

-- 8. Contagem por Banking PJ: ativa + aprovada - aceitação/passivo 
SELECT is_activity_transaction_banking, COUNT(1) AS qtd
FROM case_banking_pj.transactions_silver
GROUP BY is_activity_transaction_banking;

-- 9. Retorno apenas de Banking PJ: ativa + aprovada
SELECT *
FROM case_banking_pj.transactions_silver
WHERE is_activity_transaction_banking is TRUE;

-- 10. Retornando Banking PJ: ativa + aprovada com contagem de linha para garantir a quantidade total
SELECT 
  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS contagem_de_linha,
  ts.*
FROM case_banking_pj.transactions_silver AS ts
WHERE ts.is_activity_transaction_banking IS TRUE;

-- 11. Verificação ordenada por mês e cliente
SELECT 
id,
client_id, 
month_date_transaction, 
product_type, 
direction,
client_action, 
is_approved, 
total_value, 
is_activity_transaction_banking
FROM case_banking_pj.transactions_silver
ORDER BY month_date_transaction;

-- 12. Verificação distincts de direction e product_type
SELECT DISTINCT direction FROM case_banking_pj.transactions_silver;
SELECT DISTINCT product_type FROM case_banking_pj.transactions_silver ORDER BY 1;

-- 13. Notei que há zeros na coluna "registration_month_date" e sou substituir pelo menor número da "month_date_transaction" daquele cliente

SET SQL_SAFE_UPDATES = 0;

UPDATE case_banking_pj.transactions_silver s
JOIN (
  SELECT client_id, MIN(month_date_transaction) AS first_month
  FROM case_banking_pj.transactions_silver
  GROUP BY client_id
) f ON f.client_id = s.client_id
SET s.registration_month_date = f.first_month
WHERE s.registration_month_date = 0;

SET SQL_SAFE_UPDATES = 1;
