import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
from mailer import send_weekly_email  # <-- envia e-mail no dia configurado

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
SERIES_BRENT_ID = os.getenv("SERIES_BRENT_ID", "DCOILBRENTEU").strip()          # Brent (USD/bbl)
SERIES_DIESEL_FRED_ID = os.getenv("SERIES_DIESEL_FRED_ID", "DDFUELNYH").strip() # ULSD NY Harbor (USD/gal)
SHEET_PATH = os.getenv("SHEET_PATH", "data/planilha_unica.xlsx").strip()
EMAIL_DAY = (os.getenv("EMAIL_DAY", "FRI").strip() or "FRI").upper()
HEARTBEAT_PATH = os.getenv("HEARTBEAT_PATH", "runtime/heartbeat.json").strip()
USE_EXECUTION_DAY_FOR_EMAIL = os.getenv("USE_EXECUTION_DAY_FOR_EMAIL", "1").strip() not in {"0","false","False"}

# Constantes
GAL_TO_BBL = 42.0
REQUEST_TIMEOUT = 20
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

# ------------------------------
# Utilidades HTTP / FRED
# ------------------------------
def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def _http_get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _fred_latest_observation(series_id: str, window_days: int = 60) -> tuple[str, float]:
    """Busca a última observação numérica da série dentro de uma janela recente.
    Retorna (data_iso, valor_float)."""
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY ausente no .env")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": (datetime.utcnow() - timedelta(days=window_days)).strftime("%Y-%m-%d"),
    }
    data = _http_get(url, params)
    obs = data.get("observations", [])
    obs = [o for o in obs if o.get("value") not in (None, ".", "")]
    if not obs:
        raise RuntimeError(f"Nenhuma observação válida encontrada para {series_id}.")
    last = obs[-1]
    return last["date"][:10], float(last["value"])

# ------------------------------
# Coleta (Brent e Diesel)
# ------------------------------
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

def _is_email_day_by_date(date_iso: str) -> bool:
    """True se a data (YYYY-MM-DD) cair no dia configurado em EMAIL_DAY."""
    w = pd.Timestamp(date_iso).weekday()  # Monday=0 ... Sunday=6
    target = _DAY_MAP.get(EMAIL_DAY, 4)   # default: Friday
    return w == target

def _should_send_email(ref_date_iso: str) -> bool:
    """Decide o disparo do e-mail semanal.
    Se USE_EXECUTION_DAY_FOR_EMAIL=1 (padrão), decide pelo dia de HOJE.
    Caso contrário, usa a ref_date (como era antes)."""
    if USE_EXECUTION_DAY_FOR_EMAIL:
        today_iso = date.today().isoformat()
        return _is_email_day_by_date(today_iso)
    return _is_email_day_by_date(ref_date_iso)

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
def _write_heartbeat(success: bool, error_msg: str | None = None) -> None:
    """Escreve/atualiza o arquivo de status (heartbeat)."""
    now_local = datetime.now(timezone(timedelta(hours=-3)))  # America/Sao_Paulo
    today_str = date.today().isoformat()

    _ensure_parent_dir(HEARTBEAT_PATH)
    if os.path.exists(HEARTBEAT_PATH):
        try:
            with open(HEARTBEAT_PATH, "r", encoding="utf-8") as f:
                hb = json.load(f)
        except Exception:
            hb = {}
    else:
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
# Atualização com backfill diário
# ------------------------------
def _append_row(df: pd.DataFrame, ref_date: str, brent_bbl: float, diesel_bbl: float, email_flag: int) -> pd.DataFrame:
    spread_abs = (float(diesel_bbl) - float(brent_bbl)) if email_flag == 1 else pd.NA
    spread_pct = (float(diesel_bbl) / float(brent_bbl) - 1.0) if email_flag == 1 else pd.NA

    row = {
        "Data": ref_date,
        "Semana Anual": pd.NA,
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
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True)

def update_sheet_with_backfill(latest_brent_date: str, latest_brent_bbl: float,
                               latest_diesel_date: str, latest_diesel_bbl: float) -> str:
    """Garante atualização DIÁRIA.
    - Usa o mais recente (ref_date = max(data_brent, data_diesel)).
    - Preenche (forward-fill) os dias faltantes até HOJE com o último valor conhecido.
    - Decide o e-mail pelo dia de execução (opção padrão)."""
    # Lê ou cria a planilha
    try:
        df = pd.read_excel(SHEET_PATH)
    except FileNotFoundError:
        df = pd.DataFrame(columns=COLUMNS)

    _ensure_parent_dir(SHEET_PATH)

    ref_date = max(latest_brent_date, latest_diesel_date)
    today_iso = date.today().isoformat()

    # Se já existir a ref_date, não duplica (apenas segue para possível backfill até hoje)
    existing_dates = set(pd.to_datetime(df.get("Data", [])).dt.strftime("%Y-%m-%d")) if not df.empty else set()

    # 1) Inserir/atualizar a linha da ref_date com os valores mais recentes
    if ref_date not in existing_dates:
        email_flag = 1 if _should_send_email(ref_date) else 0
        df = _append_row(df, ref_date, latest_brent_bbl, latest_diesel_bbl, email_flag)
    else:
        # Atualiza spreads se for semana de e-mail
        if _should_send_email(ref_date):
            idx = df.index[pd.to_datetime(df["Data"]).dt.strftime("%Y-%m-%d") == ref_date][-1]
            df.at[idx, "E-mail Flag"] = 1
            df.at[idx, "Spread Absoluto Semanal (USD)"] = float(latest_diesel_bbl) - float(latest_brent_bbl)
            df.at[idx, "Diferença Relativa Semanal (%)"] = (float(latest_diesel_bbl) / float(latest_brent_bbl)) - 1.0

    # 2) Backfill: criar uma linha por dia entre (última data da planilha + 1) e HOJE
    if df.empty:
        last_date = pd.to_datetime(ref_date).date()
    else:
        last_date = max(pd.to_datetime(df["Data"]).dt.date)

    # Valor base a ser carregado para frente (últimos conhecidos)
    last_brent = latest_brent_bbl
    last_diesel = latest_diesel_bbl

    cur = last_date + timedelta(days=1)
    while cur <= pd.to_datetime(today_iso).date():
        d_iso = cur.isoformat()
        email_flag = 1 if _should_send_email(d_iso) else 0
        df = _append_row(df, d_iso, last_brent, last_diesel, email_flag)
        cur += timedelta(days=1)

    # 3) Finaliza
    df = _ensure_sheet(df)
    df = _compute_metrics(df)
    df.to_excel(SHEET_PATH, index=False)

    print(f" Planilha atualizada com sucesso em {SHEET_PATH}. Backfill diário aplicado até {today_iso}." )
    return today_iso

# ------------------------------
# Execução simples (teste manual)
# ------------------------------
if __name__ == "__main__":
    try:
        # 1) Coleta
        b_date, b_val = fetch_brent_daily_from_fred()
        d_date, d_val = fetch_diesel_daily_from_fred()
        print(f"Brent: {b_date} → {b_val:.4f} USD/bbl | Diesel: {d_date} → {d_val:.4f} USD/bbl")

        # 2) Atualiza planilha com backfill até hoje
        ref_date = update_sheet_with_backfill(b_date, b_val, d_date, d_val)

        # 3) Envio de e-mail (decisão já embutida por dia de execução)
        if _should_send_email(ref_date):
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
