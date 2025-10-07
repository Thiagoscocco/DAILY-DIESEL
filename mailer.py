import os
import smtplib
import ssl
import random
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from typing import List

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# =============================
# Configs do .env
# =============================
EMAIL_FROM = os.getenv("EMAIL_FROM", "").strip()
EMAIL_TO_PRIMARY = os.getenv("EMAIL_TO_PRIMARY", "").strip()
EMAIL_TO_SECONDARY = os.getenv("EMAIL_TO_SECONDARY", "").strip()
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").strip().lower() == "true"
EMAIL_SUBJECT_BASE = os.getenv("EMAIL_SUBJECT_BASE", "Acompanhamento Diesel & Petróleo").strip()
SHEET_PATH = os.getenv("SHEET_PATH", "data/planilha_unica.xlsx").strip()
EMAIL_DAY = (os.getenv("EMAIL_DAY", "FRI").strip() or "FRI").upper()

_DAY_MAP = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}

# =============================
# Listas para o sorteio semanal
# =============================
# Para *(____)
LIST_A_ASTERISCO = [
    "Victoria Maestri",
    "Loiras da Puc",
    "Izadora Bagio",
    "Julia Backes",
    "Bianca da Incentive Jr.",
    "Gatas da Vet",
    ]

# Para #(____)
LIST_B_HASH = [
    "Fogão 4 bocas da zootec",
    "Loirinha da Vet",
    "Dani Furacão",
    "Fofucho",
    "Gordo chaveiro",
    "Sarah Ribeiro",
    "Carol Corim",
]

# =============================
# Utilidades
# =============================
def _recipients() -> List[str]:
    recips = []
    if EMAIL_TO_PRIMARY:
        recips.append(EMAIL_TO_PRIMARY)
    if EMAIL_TO_SECONDARY:
        recips.append(EMAIL_TO_SECONDARY)
    if not recips:
        raise RuntimeError("Nenhum destinatário definido (EMAIL_TO_PRIMARY/EMAIL_TO_SECONDARY)")
    return recips


def _fmt_money(x):
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "-"


def _fmt_pct(x):
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return "-"


def _compose_subject(ref_date: str) -> str:
    # ref_date: YYYY-MM-DD
    return f"{EMAIL_SUBJECT_BASE} – Semana de {ref_date}"


def _weekly_seed(date_str: str) -> int:
    """
    Gera uma semente determinística por semana ISO,
    para o sorteio ser reprodutível dentro da mesma semana.
    """
    dt = pd.to_datetime(date_str)
    iso = dt.isocalendar()  # tem .year e .week
    return int(f"{iso.year}{int(iso.week):02d}")


def _pick_weekly(name_list: List[str], seed: int, salt: int) -> str:
    rng = random.Random(seed + salt)
    return rng.choice(name_list) if name_list else "—"


def _compose_body(df: pd.DataFrame) -> str:
    if df.empty:
        return "Planilha vazia."

    # Última linha como referência
    last = df.iloc[-1]
    data = str(last.get("Data", "-"))
    brent = _fmt_money(last.get("Petróleo Barril (USD)", "-"))
    diesel = _fmt_money(last.get("Diesel Barril (USD)", "-"))
    var_b = _fmt_pct(last.get("Variação Petróleo (%)", None))
    var_d = _fmt_pct(last.get("Variação Diesel (%)", None))
    spread_abs = last.get("Spread Absoluto Semanal (USD)", None)
    spread_pct = last.get("Diferença Relativa Semanal (%)", None)

    spread_abs_s = _fmt_money(spread_abs) if pd.notna(spread_abs) else "-"
    spread_pct_s = _fmt_pct(spread_pct) if pd.notna(spread_pct) else "-"

    # Sorteio semanal reprodutível
    seed = _weekly_seed(data)
    sorteado_asterisco = _pick_weekly(LIST_A_ASTERISCO, seed, salt=1)
    sorteado_hash = _pick_weekly(LIST_B_HASH, seed, salt=2)

    # Cabeçalho customizado solicitado
    cabecalho = (
        "Boa Tarde Thiago e Gustavo, segue a atualização semanal dos preços do Diesel e Petróleo.\n\n"
        "Copada da semana:\n"
        f"Thigas: {sorteado_asterisco}\n"
        f"Gustagol: {sorteado_hash}\n\n\n"
        "Não Afrouxemo\n"
    )

    resumo = "\n".join(
        [
            "",
            f"Data de referência: {data}",
            f"Brent (USD/bbl): {brent}  |  Diesel (USD/bbl): {diesel}",
            f"Variação diária – Brent: {var_b}  |  Diesel: {var_d}",
            f"Spread semanal (USD): {spread_abs_s}  |  Diferença relativa: {spread_pct_s}",
            "",
            "Planilha completa em anexo.",
        ]
    )

    return cabecalho + resumo


def _attach_file(msg: MIMEMultipart, file_path: str, attach_name: str | None = None) -> None:
    with open(file_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    filename = attach_name or os.path.basename(file_path)
    part.add_header("Content-Disposition", f"attachment; filename=\"{filename}\"")
    msg.attach(part)


def send_weekly_email(sheet_path: str | None = None) -> None:
    sheet = sheet_path or SHEET_PATH
    if not os.path.exists(sheet):
        raise FileNotFoundError(f"Planilha não encontrada: {sheet}")

    # Carrega planilha
    df = pd.read_excel(sheet)

    # Data para o assunto: última linha
    ref_date = str(df.iloc[-1]["Data"]) if not df.empty else datetime.today().strftime("%Y-%m-%d")

    # Monta e-mail
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(_recipients())
    msg["Subject"] = _compose_subject(ref_date)
    msg.attach(MIMEText(_compose_body(df), "plain"))

    # Anexo
    _attach_file(msg, sheet)

    # Envio SMTP (TLS por padrão)
    if SMTP_USE_TLS and SMTP_PORT != 465:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(EMAIL_FROM, _recipients(), msg.as_string())
    else:
        # SSL direto (porta 465)
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(EMAIL_FROM, _recipients(), msg.as_string())

    print("✅ E-mail enviado com sucesso para:", ", ".join(_recipients()))


if __name__ == "__main__":
    # Uso manual: envia o e-mail agora com a planilha configurada no .env
    try:
        send_weekly_email()
    except Exception as e:
        print("❌ Erro ao enviar e-mail:", e)
