📊 Análise de Impacto de Cupons na Retenção e Frequência de Compras
Sobre o Projeto
Este repositório contém a análise e os resultados de um case study sobre o impacto de uma campanha de cupons no comportamento de compra dos clientes.
Desenvolvido utilizando Databricks, o estudo avalia se a distribuição de cupons em janeiro gerou efeitos positivos na retenção e frequência de compras, utilizando métodos comparativos entre grupos de teste e controle.

🔍 Metodologia
- Análise comparativa entre grupo teste (recebeu cupons) e grupo controle
- Segmentação de clientes por lifecycle (Ativos, Reativados)
- Métricas de frequência de compra e comportamento de consumo
- Pipeline de processamento de dados em Databricks/PySpark
- Visualizações estatísticas através de boxplots e gráficos comparativos

📈 Principais Descobertas
- Diferença positiva de 7p.p na proporção de usuários com aumento na frequência para o grupo teste
- Crescimento equivalente (96%) no volume total de pedidos em ambos os grupos
- Incremento financeiro limitado, com apenas 1p.p de diferencial no GMV
- Distribuição de ticket médio praticamente idêntica entre os grupos
- Evidências de possível viés de segmentação inicial nos grupos de teste

🚀 Recomendações para Testes Futuros
- Melhorar segmentação: Implementar pareamento por histórico de compras para grupos verdadeiramente comparáveis
- Ajustar período: Selecionar meses com menor influência sazonal que dezembro/janeiro
- Diversificar estratégias: Testar diferentes valores e tipos de cupons por segmento
- Estender duração: Ampliar o período de análise para capturar efeitos de médio prazo
- Medir impacto completo: Avaliar não apenas frequência, mas também retenção e LTV

🔧 Tecnologias Utilizadas
Databricks como ambiente de desenvolvimento
PySpark para processamento de dados em larga escala
Python (pandas, matplotlib, seaborn) para análises e visualizações
SQL para transformação e agregação de dados

📝 Conclusão
Este estudo demonstra a importância de uma segmentação adequada e consideração de fatores sazonais em testes de marketing.
Apesar dos cupons gerarem um aumento na proporção de usuários com maior frequência de compra, o impacto financeiro foi limitado, sugerindo a necessidade de estratégias mais personalizadas e períodos de teste mais adequados para futuras campanhas.
