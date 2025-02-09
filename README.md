# README: Arquitetura do Fluxo de Análise de Ações

## Visão Geral

Este projeto implementa um fluxo automatizado para análise de ações utilizando o framework **Prefect**. O fluxo coleta dados históricos de ações da B3 (Bolsa de Valores do Brasil) via Yahoo Finance, calcula indicadores técnicos, salva os dados localmente e no Google Drive, gera relatórios gráficos e registra informações sobre as maiores variações diárias. Além disso, o código é sincronizado automaticamente com um repositório GitHub.

---

## Arquitetura do Fluxo

### 1. Coleta de Dados
- Utiliza a biblioteca `yfinance` para baixar dados históricos das últimas 7 dias para cada ticker.
- Os tickers são uma lista pré-definida de ações da B3.

### 2. Cálculo de Indicadores
- Calcula a **Média Móvel (MA)** e a **Volatilidade** dos preços de fechamento (`Close`) ao longo de uma janela de 5 dias.

### 3. Armazenamento Local
- Salva os dados em arquivos CSV organizados por data (`data/YYYY-MM-DD/`).

### 4. Upload para o Google Drive
- Autentica-se no Google Drive usando OAuth2 e faz upload dos arquivos CSV.

### 5. Geração de Relatórios
- Cria gráficos mostrando o preço de fechamento e a média móvel para cada ticker.
- Salva os gráficos como imagens PNG.

### 6. Registro de Movimentações
- Identifica as três maiores altas e quedas diárias com base na variação percentual (`Daily_Return`).
- Registra essas informações como artefatos no Prefect.

### 7. Sincronização com GitHub
- Faz commit e push automáticos das alterações para o repositório GitHub configurado.

### 8. Agendamento
- O fluxo é executado diariamente às 22h (UTC) usando um agendador Cron.

![alt text](image.png)