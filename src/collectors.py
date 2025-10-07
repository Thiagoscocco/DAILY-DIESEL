import os
import time
import requests
from datetime import datetime
from typing import Tuple, Optional

# Conversão de unidade
GALLON_PER_BARREL = 42.0

class EIAClient:
    """
    Cliente simples para EIA Open Data (endpoint /series).
    Usa series_id vindos do .env (ex.: PET.RBRTE.D para Brent diário).
    """

    def __init__(self, api_key: str, max_retries: int = 3, backoff_seconds: int = 5):
        self.api_key = api_key
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.base_url = "https://api.eia.gov/series/"

    def _request(self, series_id: str) -> dict:
        params = {"api_key": self.api_key, "series_id": series_id}
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = requests.get(self.base_url, params=params, timeout=20)
                if r.status_code == 200:
                    return r.json()
                last_err = RuntimeError(f"HTTP {r.status_code} - {r.text[:200]}")
            except Exception as e:
                last_err = e
            time.sleep(self.backoff_seconds * (3 ** (attempt - 1)))  # backoff exponencial: 5s,15s,45s...
        raise RuntimeError(f"Falha ao consultar EIA para série {series_id}: {last_err}")

    @staticmethod
    def _parse_eia_date(raw_date: str) -> str:
        """
        Converte datas EIA em ISO YYYY-MM-DD.
        Exemplos possíveis:
          - '20250821'  -> '2025-08-21'
          - '2025-08-21'-> '2025-08-21' (já no formato)
          - '202508'    -> '2025-08-01'  (mensal: assume dia 1)
          - '2025'      -> '2025-01-01'  (anual: assume 1/jan)
        """
        raw = str(raw_date)
        if len(raw) == 10 and "-" in raw:
            return raw  # já está em ISO
        if len(raw) == 8 and raw.isdigit():
            return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
        if len(raw) == 6 and raw.isdigit():
            return f"{raw[0:4]}-{raw[4:6]}-01"
        if len(raw) == 4 and raw.isdigit():
            return f"{raw}-01-01"
        return raw  # fallback

    def get_latest_point(self, series_id: str) -> Tuple[str, float]:
        """
        Retorna (data_iso, valor) mais recente disponível para a série.
        """
        payload = self._request(series_id)
        try:
            series = payload["series"][0]
            data = series["data"]  # lista de pares [date, value], do mais novo para o mais antigo
            if not data:
                raise RuntimeError("Série sem dados")
            date_raw, value = data[0]
            date_iso = self._parse_eia_date(date_raw)
            value_float = float(value)
            return date_iso, value_float
        except Exception as e:
            raise RuntimeError(f"Erro ao interpretar resposta da série {series_id}: {e}") from e


def get_today_prices_from_env() -> dict:
    """
    Lê IDs das séries no .env e retorna preços do dia (último disponível):
      - 'Petróleo Barril (USD)' já vem em USD/bbl
      - 'Diesel Barril (USD)' é convertido de USD/galão → USD/bbl (×42) se necessário
    Retorno:
      {
        "date": "YYYY-MM-DD",
        "brent_usd_bbl": float,
        "diesel_usd_bbl": float,
        "sources": {"brent": SERIES_BRENT_ID, "diesel": SERIES_DIESEL_ID}
      }
    """
    api_key = os.getenv("EIA_API_KEY", "").strip()
    series_brent = os.getenv("SERIES_BRENT_ID", "").strip()
    series_diesel = os.getenv("SERIES_DIESEL_ID", "").strip()
    if not api_key or not series_brent or not series_diesel:
        raise RuntimeError("Verifique .env: faltam EIA_API_KEY, SERIES_BRENT_ID ou SERIES_DIESEL_ID.")

    diesel_unit = (os.getenv("DIESEL_UNIT", "GAL").strip().upper() or "GAL")  # GAL (padrão) ou BBL

    client = EIAClient(api_key=api_key,
                       max_retries=int(os.getenv("MAX_RETRIES", "3")),
                       backoff_seconds=int(os.getenv("RETRY_BACKOFF_SECONDS", "5")))

    # Brent (esperado já em USD/bbl)
    brent_date, brent_value = client.get_latest_point(series_brent)

    # Diesel (muito comum vir em USD/galão -> converter)
    diesel_date, diesel_value = client.get_latest_point(series_diesel)

    # Escolhe a data mais recente entre as duas séries como referência
    ref_date = max(brent_date, diesel_date)

    if diesel_unit == "GAL":
        diesel_usd_bbl = diesel_value * GALLON_PER_BARREL
    elif diesel_unit == "BBL":
        diesel_usd_bbl = diesel_value
    else:
        # fallback seguro: assumir GAL se vier algo inesperado
        diesel_usd_bbl = diesel_value * GALLON_PER_BARREL

    return {
        "date": ref_date,
        "brent_usd_bbl": float(brent_value),
        "diesel_usd_bbl": float(diesel_usd_bbl),
        "sources": {"brent": series_brent, "diesel": series_diesel},
    }
