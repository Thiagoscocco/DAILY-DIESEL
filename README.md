## Daily Diesel

Aplicativo Python para coletar preços diários de Diesel e Petróleo (FRED), feito inicialmenta para coleta de dados de um trabalho de conclusão de semestre na disciplina de mercados agrícolas, do oitavo semestre do curso de Agronomia da Universidade Federal do Rio Grande do Sul. Busca atualizar uma planilha e enviar e-mails com o relatório semanal. Nova UI em `customtkinter` com tema escuro.

### Requisitos
- Python 3.10+
- Dependências do `requirements.txt`

### Instalação
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Configuração
Crie um arquivo `.env` na raiz, baseado em `ENV.EXAMPLE`.

### Uso
- Coleta/planilha via interface gráfica:
```bash
python gui.py
```
- Execução headless (coleta, atualiza planilha e envia e-mail se for o dia configurado):
```bash
python main.py
```
- Enviar e-mail manualmente (usa planilha em `SHEET_PATH`):
```bash
python mailer.py
```

### Estrutura
- `main.py`: coleta do FRED, atualização da planilha, heartbeat e envio condicional de e-mail
- `mailer.py`: composição e envio do e-mail com anexo
- `gui.py`: interface com `customtkinter` (tema escuro, botões azuis)
- `data/planilha_unica.xlsx`: planilha de saída
- `runtime/heartbeat.json`: status de última execução

### Variáveis de Ambiente
Consulte `.env.example` para a lista completa. Principais:
- `FRED_API_KEY`: chave da API do FRED
- `SHEET_PATH`: caminho da planilha (padrão: `data/planilha_unica.xlsx`)
- `EMAIL_FROM`, `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
- `EMAIL_TO_PRIMARY`, `EMAIL_TO_SECONDARY` (opcional; a UI permite sobrescrever)
- `EMAIL_SUBJECT_BASE` (padrão: "Acompanhamento Diesel & Petróleo")
- `EMAIL_DAY` (MON..SUN; padrão: FRI)

### Licença
MIT


