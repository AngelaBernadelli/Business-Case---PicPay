SQL
-- 1. Criação da tabela transacionais_gold dentro do schema case_banking_pj vinda da camada silver 
CREATE TABLE case_banking_pj.transactions_gold (
  client_id INT NOT NULL COMMENT 'Identificador único do cliente',
  month_date_transaction INT NOT NULL COMMENT 'Mês de referência das transações',
  registration_month_date INT NOT NULL COMMENT 'Mês em que o cliente abriu a conta',
  total_tpv_in_banking DECIMAL(18,2) COMMENT 'Valor financeiro que entrou (IN) de transações aprovadas Banking PJ',
  total_tpv_out_banking DECIMAL(18,2) COMMENT 'Valor que saiu (OUT) de transações aprovadas Banking PJ',
  total_tpv_in_outros DECIMAL(18,2) COMMENT 'Valor que entrou (IN) de transações aprovadas que não são Banking',
  total_tpv_out_outros DECIMAL(18,2) COMMENT 'Valor que saiu (OUT) de transações aprovadas que não são Banking',
  is_active_banking BOOLEAN COMMENT 'TRUE para identificar se o cliente foi ativo em transações aprovadas de Banking no mês',
  PRIMARY KEY (client_id, month_date_transaction) COMMENT 'Chave Primária'
);

-- 2. Popular a tabela
INSERT INTO case_banking_pj.transactions_gold
WITH

-- 3. Criando a CTEs com a regra que atenda a 'total_tpv_in_banking' e 'total_tpv_out_banking'
  banking_pj AS (
    SELECT 
		client_id, 
		month_date_transaction, 
		registration_month_date,
		direction, total_value, 
		is_activity_transaction_banking
    
    FROM case_banking_pj.transactions_silver
    
    WHERE is_approved = TRUE
      AND product_type IN (
        'EMISSAO_BOLETO',
        'PAGAMENTO_BOLETO',
        'CARTAO_CREDITO',
        'CARTAO_DEBITO',
        'COFRINHO_APORTE_MANUAL',
        'COFRINHO_RESGATE_AUTO',
        'PIX_SALE','PIX_TRANSFER_KEY',
        'PIX_TRANSFER_KEY_RECEIVEMENT'
      )
  ),
  
  -- 4. Definindo 'direction' para cada situação. 'IN' para entrada e 'OUT' para saídas. Também já criamos a última linha 'is_active_banking' com a função 'MAX'
  banking_aggregate AS (
    SELECT
      client_id,
      month_date_transaction,
      MIN(registration_month_date) AS registration_month_date,
      SUM(CASE WHEN direction='IN'  THEN total_value ELSE 0 END) AS total_tpv_in_banking,
      SUM(CASE WHEN direction='OUT' THEN total_value ELSE 0 END) AS total_tpv_out_banking,
      MAX(CASE WHEN is_activity_transaction_banking = TRUE THEN 1 ELSE 0 END) AS is_active_banking
    FROM banking_pj
    GROUP BY client_id, month_date_transact_
