# Business Case – PicPay

**Pipeline Analítica com MySQL | Banking PJ**

---

## 🎯 Objetivo

Desenvolver uma **pipeline analítica SQL** para o **case Banking PJ**, estruturando os dados em camadas (Bronze → Silver → Gold) e produzindo indicadores sobre o **comportamento de clientes PJ** em produtos bancários, com foco em:

* Identificar **clientes ativos em Banking** ao longo dos meses;
* Calcular **entradas e saídas financeiras (TPV)**;
* Classificar os clientes em **novos, ativados, recorrentes e reativados**;
* Entregar uma **visão consolidada mensal** do ciclo de vida desses clientes.

---

## 🧭 Etapas do Projeto

| Etapa       | Camada / Foco            | Descrição                                                                                                    |
| ----------- | ------------------------ | ------------------------------------------------------------------------------------------------------------ |
| **Etapa 0** | Introdução e Arquitetura | Estruturação do case, ferramentas, boas práticas e modelo de camadas.                                        |
| **Etapa 1** | Bronze                   | Criação da tabela `transactions_bronze` e ingestão do arquivo CSV bruto.                                     |
| **Etapa 2** | Silver → Gold            | Normalização dos dados, criação de `is_activity_transaction_banking` e agregações mensais.                   |
| **Etapa 3** | Métricas                 | Contagem de clientes ativos por mês (`is_active_banking = TRUE`).                                            |
| **Etapa 4** | Ciclo de Vida            | Identificação de clientes novos, ativados, recorrentes e reativados. Pivot final consolidando os resultados. |

---

## 🧱 Arquitetura da Pipeline

```text
                ┌────────────────────────┐
                │     Fonte Original     │
                │   (transactions_v5.csv)│
                └────────────┬───────────┘
                             │
                             ▼
                 ┌────────────────────────┐
                 │        BRONZE          │
                 │ Dados brutos, sem      │
                 │ transformações.        │
                 └────────────┬───────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │        SILVER          │
                 │ Limpeza, tipagem,      │
                 │ e flag de atividade.   │
                 └────────────┬───────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │         GOLD           │
                 │ Agregações, métricas   │
                 │ e indicadores finais.  │
                 └────────────────────────┘
```

---

## ⚙️ Ferramentas Utilizadas

| Ferramenta                | Finalidade                                                 |
| ------------------------- | ---------------------------------------------------------- |
| **MySQL Workbench**       | Execução das queries, modelagem e verificação das tabelas. |
| **SQL (DDL / DML / CTE)** | Criação das tabelas, agregações e manipulação de dados.    |
| **Excel**                 | Validação cruzada de métricas e resultados.                |
| **GitHub Wiki**           | Documentação detalhada de cada etapa do projeto.           |

📖 [Acesse a Wiki Completa do Projeto](https://github.com/AngelaBernadelli/Business-Case---PicPay/wiki)

---

## 🧩 Principais Consultas SQL

### 1. Camada Bronze – Criação da tabela base

```sql
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
```

**Motivo:**
A tabela Bronze é o ponto de ingestão dos dados originais (.csv), garantindo rastreabilidade e reprocessamento futuro sem perda de informação.

---

### 2. Camada Silver – Limpeza e criação da flag `is_activity_transaction_banking`

Trecho principal:

```sql
INSERT INTO case_banking_pj.transactions_silver (...)
SELECT
  CAST(TRIM(b.client_id) AS UNSIGNED) AS client_id,
  ...
  (LOWER(TRIM(b.is_approved)) = 'true') AND
  (LOWER(TRIM(b.client_action)) = 'true') AND
  (
    b.product_type LIKE 'PIX_%'
    OR b.product_type LIKE 'EMISSAO_BOLETO%'
    ...
  ) AS is_activity_transaction_banking
FROM case_banking_pj.transactions_bronze b;
```

**Motivo:**
A Silver garante padronização dos tipos, limpeza textual e definição de quais transações realmente representam **atividade bancária (Banking PJ)**.

Inclui correções em campos como `registration_month_date` (zeros substituídos pelo primeiro mês de transação do cliente).

---

### 3. Camada Gold – Agregações Mensais

```sql
WITH banking_pj AS (...),
banking_aggregate AS (...),
outher AS (...),
out_aggregate AS (...)
SELECT
  b.client_id,
  b.month_date_transaction,
  b.registration_month_date,
  b.total_tpv_in_banking,
  b.total_tpv_out_banking,
  COALESCE(o.total_tpv_in_outros, 0) AS total_tpv_in_outros,
  COALESCE(o.total_tpv_out_outros, 0) AS total_tpv_out_outros,
  b.is_active_banking
FROM banking_aggregate b
LEFT JOIN out_aggregate o
  ON o.client_id = b.client_id
 AND o.month_date_transaction = b.month_date_transaction;
```

**Motivo:**
A Gold concentra as métricas finais — **entradas e saídas financeiras (TPV)** e a flag **is_active_banking**, que indica clientes com transações ativas no período.
Os `COALESCE` tratam valores nulos, garantindo consistência financeira (sem transação = zero).

---

### 4. Métricas Mensais – Clientes Ativos

```sql
SELECT
  month_date_transaction,
  COUNT(DISTINCT client_id) AS clientes_ativos_mes
FROM case_banking_pj.transactions_gold
WHERE is_active_banking = TRUE
GROUP BY month_date_transaction;
```

**Motivo:**
Identifica o **número total de clientes ativos** em produtos Banking PJ por mês, servindo de base para as análises de ciclo de vida.

---

### 5. Ciclo de Vida dos Clientes – Classificação e Pivot Final

A consulta utiliza **funções de janela (LAG, MIN, SUM)** e **CASE WHEN** para categorizar clientes:

* **Novos:** `registration_month_date = month_date_transaction`
* **Recorrentes:** transacionaram também no mês anterior
* **Ativados:** clientes que abriram conta antes, mas ficaram ativos pela 1ª vez no mês
* **Reativados:** clientes que voltaram a transacionar após pelo menos 1 mês inativo

```sql
WITH client_month_by_month AS (...),
resumo_por_mes AS (...)
SELECT
  'Novos clientes' AS categoria, ...
UNION ALL
SELECT
  'Recorrentes' AS categoria, ...
UNION ALL
SELECT
  'Ativados' AS categoria, ...
UNION ALL
SELECT
  'Reativados' AS categoria, ...
UNION ALL
SELECT
  'Total - Clientes ativos em Banking' AS categoria, ...
FROM resumo_por_mes;
```

**Motivo:**
Essa estrutura entrega uma **visão matricial (pivot)** com os valores de cada categoria em colunas (mês 1, mês 2, mês 3), consolidando o ciclo de vida completo dos clientes Banking PJ.

---

## 📈 Resultados e Interpretação

| Categoria          | Definição                                                       | Insight Esperado           |
| ------------------ | --------------------------------------------------------------- | -------------------------- |
| **Novos Clientes** | Abriram conta e transacionaram no mesmo mês.                    | Entrada de novos clientes. |
| **Ativados**       | Abriram conta em mês anterior e começaram a transacionar agora. | Engajamento inicial.       |
| **Recorrentes**    | Mantêm atividade mês a mês.                                     | Fidelização.               |
| **Reativados**     | Voltaram após um período de inatividade.                        | Retenção / reengajamento.  |
| **Total Ativos**   | Soma de todos os grupos ativos.                                 | Base consolidada.          |

Os resultados mensais do pivot mostraram coerência com os totais da Tarefa 3, comprovando consistência no pipeline.

---

## 📦 Entrega e Resultados Visuais

Os resultados visuais das tabelas e do ciclo de clientes estão disponíveis na aba
👉 [**Issues do Repositório**](https://github.com/AngelaBernadelli/Business-Case---PicPay/issues)

Lá você encontra:

* Visualizações das tabelas **Bronze**, **Silver**, **Gold** e **Pivot Final**
* Imagens com amostras das bases processadas
* E o link direto para download do pacote `.zip` contendo:

  * Scripts SQL (`.sql`)
  * Tabelas exportadas (`.csv`)
  * Documentação auxiliar (`.txt`)

📥 **Download:** [Case - Analytics - Banking PJ - v2 - entrega.zip](https://github.com/user-attachments/files/23442198/Case.-.Analytics.-.Banking.PJ.-.v2.-.entrega.zip)

---


## 📂 Estrutura de Arquivos

```
business_case_picpay/
│
├── etapa_1_bronze.sql
├── etapa_2_gold.sql
├── etapa_3_clientes_ativos.sql
├── etapa_4_ciclo_de_vida.sql
├── README.md
```

---

## 📚 Fonte de Dados

O arquivo original (`transactions_v5.csv`) foi fornecido no material do **Business Case – Analytics | PicPay**, contendo dados anonimizados de transações de **clientes PJ**, direções (`IN/OUT`), produtos, aprovações e valores.

---

📘 [Acesse a Wiki Completa do Projeto](https://github.com/AngelaBernadelli/Business-Case---PicPay/wiki)

---

## 👩‍💻 Autora

**Angela Bernadelli**
[LinkedIn](https://www.linkedin.com/in/angela-bernadelli/)
