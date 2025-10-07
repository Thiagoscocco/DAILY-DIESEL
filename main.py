import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from mailer import send_weekly_email 

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
SERIES_BRENT_ID = os.getenv("SERIES_BRENT_ID", "DCOILBRENTEU").strip()         
SERIES_DIESEL_FRED_ID = os.getenv("SERIES_DIESEL_FRED_ID", "DDFUELNYH").strip() 
SHEET_PATH = os.getenv("SHEET_PATH", "data/planilha_unica.xlsx").strip()
EMAIL_DAY = (os.getenv("EMAIL_DAY", "FRI").strip() or "FRI").upper()
HEARTBEAT_PATH = os.getenv("HEARTBEAT_PATH", "runtime/heartbeat.json").strip()

REQUEST_TIMEOUT = 20
GAL_TO_BBL = 42.0 

COLUMNS = [
    "Data",
    "Semana Anual",
    "Petróleo Barril (USD)",
    "Diesel Barril (USD)",
    "Variação Petróleo (%)",
    "Variação Diesel (%)",
    "Média Móvel semanal Petróleo",
    "Média móvel mensal Petróleo",
    "Média móvel Semanal Diesel",
    "Média Móvel Mensal Diesel",
    "E-mail Flag",
    "Spread Absoluto Semanal (USD)",
    "Diferença Relativa Semanal (%)",
]

def _http_get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _fred_latest_observation(series_id: str, window_days: int = 40) -> tuple[str, float]:
    """
    Busca no FRED a última observação numérica da série dentro de uma janela recente.
    Retorna (data_iso, valor_float).
    """
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY ausente no .env")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat(),
        "sort_order": "desc",
        "limit": 20,
    }
    js = _http_get(url, params)
    for obs in js.get("observations", []):
        v = obs.get("value")
        if v is not None and v != ".":
            return obs["date"], float(v)
    raise RuntimeError(f"Sem observações numéricas recentes no FRED para {series_id}")

def fetch_brent_daily_from_fred() -> tuple[str, float]:
    """Brent (USD/bbl) via FRED (DCOILBRENTEU por padrão)."""
    return _fred_latest_observation(SERIES_BRENT_ID)

def fetch_diesel_daily_from_fred() -> tuple[str, float]:
    """Diesel ULSD (USD/gal) via FRED → converte para USD/bbl (×42)."""
    d_date, d_gal = _fred_latest_observation(SERIES_DIESEL_FRED_ID)
    return d_date, float(d_gal) * GAL_TO_BBL

# ------------------------------
# Suporte: email-day / semana
# ------------------------------
_DAY_MAP = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}

def _is_email_day(date_iso: str) -> bool:
    """True se a data (YYYY-MM-DD) cair no dia configurado em EMAIL_DAY."""
    w = pd.Timestamp(date_iso).weekday()  # Monday=0 ... Sunday=6
    target = _DAY_MAP.get(EMAIL_DAY, 4)   # default: Friday
    return w == target

# ------------------------------
# Planilha: garantir estrutura e calcular métricas
# ------------------------------
def _ensure_sheet(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[COLUMNS]

def _compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # Tipos de data e semana ISO
    df["Data"] = pd.to_datetime(df["Data"]).dt.date
    iso = pd.to_datetime(df["Data"]).dt.isocalendar()
    df["Semana Anual"] = iso.week.astype(int)

    # Variações diárias (fracionárias)
    df["Variação Petróleo (%)"] = pd.to_numeric(df["Petróleo Barril (USD)"], errors="coerce").pct_change()
    df["Variação Diesel (%)"] = pd.to_numeric(df["Diesel Barril (USD)"], errors="coerce").pct_change()

    # Médias móveis (7 e 30 dias)
    df["Média Móvel semanal Petróleo"] = pd.to_numeric(df["Petróleo Barril (USD)"], errors="coerce").rolling(7, min_periods=7).mean()
    df["Média móvel mensal Petróleo"] = pd.to_numeric(df["Petróleo Barril (USD)"], errors="coerce").rolling(30, min_periods=30).mean()
    df["Média móvel Semanal Diesel"] = pd.to_numeric(df["Diesel Barril (USD)"], errors="coerce").rolling(7, min_periods=7).mean()
    df["Média Móvel Mensal Diesel"] = pd.to_numeric(df["Diesel Barril (USD)"], errors="coerce").rolling(30, min_periods=30).mean()

    # Colunas semanais numéricas
    for col in ["E-mail Flag", "Spread Absoluto Semanal (USD)", "Diferença Relativa Semanal (%)"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# ------------------------------
# Heartbeat (status)
# ------------------------------
def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def _write_heartbeat(success: bool, error_msg: str | None = None) -> None:
    """
    Escreve/atualiza o arquivo de status (heartbeat).
    Em sucesso: atualiza last_run e last_success e limpa erro.
    Em erro: atualiza last_run, last_error e last_error_msg.
    """
    _ensure_parent_dir(HEARTBEAT_PATH)

    now_local = datetime.now(timezone.utc).astimezone()
    today_str = now_local.date().isoformat()

    hb = {}
    if os.path.exists(HEARTBEAT_PATH):
        try:
            with open(HEARTBEAT_PATH, "r", encoding="utf-8") as f:
                hb = json.load(f)
        except Exception:
            hb = {}

    hb["last_run"] = now_local.isoformat()

    if success:
        hb["last_success"] = today_str
        hb["last_error"] = ""
        hb["last_error_msg"] = ""
    else:
        hb["last_error"] = today_str
        hb["last_error_msg"] = str(error_msg) if error_msg else "Erro não especificado"

    with open(HEARTBEAT_PATH, "w", encoding="utf-8") as f:
        json.dump(hb, f, ensure_ascii=False, indent=2)

# ------------------------------
# Atualizar a planilha com a linha do dia
# ------------------------------
def update_sheet(brent_date: str, brent_bbl: float, diesel_date: str, diesel_bbl: float) -> str:
    """
    Atualiza a planilha com os valores do dia.
    Retorna a data de referência (string YYYY-MM-DD) que foi usada no registro.
    """
    # Lê ou cria a planilha
    try:
        df = pd.read_excel(SHEET_PATH)
    except FileNotFoundError:
        df = pd.DataFrame(columns=COLUMNS)

    # Data do registro: usamos a mais recente disponível entre as duas séries
    ref_date = max(brent_date, diesel_date)

    # Evita duplicar a mesma Data
    if "Data" in df.columns and not df.empty:
        if any(pd.to_datetime(df["Data"]).dt.strftime("%Y-%m-%d") == ref_date):
            # Mesmo assim, se for dia de e-mail e spreads estiverem vazios, atualizar spreads
            idx = df.index[pd.to_datetime(df["Data"]).dt.strftime("%Y-%m-%d") == ref_date]
            if len(idx) > 0 and _is_email_day(ref_date):
                i = idx[-1]
                df.at[i, "E-mail Flag"] = 1
                df.at[i, "Spread Absoluto Semanal (USD)"] = float(diesel_bbl) - float(brent_bbl)
                df.at[i, "Diferença Relativa Semanal (%)"] = (float(diesel_bbl) / float(brent_bbl)) - 1.0
                df = _compute_metrics(df)
                df.to_excel(SHEET_PATH, index=False)
                print(f"ℹ Data {ref_date} já existia; spreads semanais atualizados (dia do e-mail).")
            else:
                print(f"ℹ Data {ref_date} já registrada; nada a fazer.")
            return ref_date

    # Monta a nova linha
    email_flag = 1 if _is_email_day(ref_date) else 0
    spread_abs = (float(diesel_bbl) - float(brent_bbl)) if email_flag == 1 else pd.NA
    spread_pct = (float(diesel_bbl) / float(brent_bbl) - 1.0) if email_flag == 1 else pd.NA

    new_row = {
        "Data": ref_date,
        "Semana Anual": pd.NA,  # calculado em _compute_metrics
        "Petróleo Barril (USD)": float(brent_bbl),
        "Diesel Barril (USD)": float(diesel_bbl),
        "Variação Petróleo (%)": pd.NA,
        "Variação Diesel (%)": pd.NA,
        "Média Móvel semanal Petróleo": pd.NA,
        "Média móvel mensal Petróleo": pd.NA,
        "Média móvel Semanal Diesel": pd.NA,
        "Média Móvel Mensal Diesel": pd.NA,
        "E-mail Flag": email_flag,
        "Spread Absoluto Semanal (USD)": spread_abs,
        "Diferença Relativa Semanal (%)": spread_pct,
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df = _ensure_sheet(df)
    df = _compute_metrics(df)

    df.to_excel(SHEET_PATH, index=False)
    msg_week = " (fechamento semanal)" if email_flag == 1 else ""
    print(f" Planilha atualizada com sucesso em {SHEET_PATH}{msg_week}")
    return ref_date

# ------------------------------
# Execução simples (teste manual)
# ------------------------------
if __name__ == "__main__":
    try:
        # 1) Coleta
        b_date, b_val = fetch_brent_daily_from_fred()
        d_date, d_val = fetch_diesel_daily_from_fred()
        print(f"Brent: {b_date} → {b_val:.4f} USD/bbl | Diesel: {d_date} → {d_val:.4f} USD/bbl")

        # 2) Atualiza planilha e pega a data usada
        ref_date = update_sheet(b_date, b_val, d_date, d_val)

        # 3) Se hoje é o dia do e-mail, enviar com a planilha anexa
        if _is_email_day(ref_date):
            try:
                send_weekly_email(SHEET_PATH)
            except Exception as e:
                print(f" Erro ao enviar e-mail: {e}")
                _write_heartbeat(success=False, error_msg=e)
            else:
                _write_heartbeat(success=True)
        else:
            _write_heartbeat(success=True)

    except Exception as e:
        print(f" Erro no processo: {e}")
        _write_heartbeat(success=False, error_msg=e)
