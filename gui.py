import os
import json
import threading
import traceback
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from dotenv import load_dotenv

# Tentamos usar Pillow para redimensionar PNGs; se não houver, usamos PhotoImage do Tk
try:
    from PIL import Image, ImageTk  # pip install pillow
    PIL_OK = True
except Exception:
    PIL_OK = False

# ----------------------------------
# Config
# ----------------------------------
load_dotenv()
HEARTBEAT_PATH = os.getenv("HEARTBEAT_PATH", "runtime/heartbeat.json").strip()
SHEET_PATH = os.getenv("SHEET_PATH", "data/planilha_unica.xlsx").strip()
LOGO1_PATH = os.getenv("LOGO1_PATH", "assets/logo1.png").strip()
LOGO2_PATH = os.getenv("LOGO2_PATH", "assets/logo2.png").strip()

REFRESH_MS = 10_000  # 10 segundos

# ----------------------------------
# Utilidades
# ----------------------------------
def read_heartbeat() -> dict:
    try:
        if os.path.exists(HEARTBEAT_PATH):
            with open(HEARTBEAT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def status_from_heartbeat(hb: dict) -> tuple[str, str]:
    """
    (texto_status, cor_hex)
    Em Operação (verde) se last_success mais recente que last_error (ou sem erro).
    Fora de Operação (vermelho) se last_error >= last_success ou sem success.
    """
    last_success = hb.get("last_success", "") or ""
    last_error = hb.get("last_error", "") or ""
    if last_success and (not last_error or last_error < last_success):
        return "Em Operação", "#1f9d55"  # verde
    if last_error and (not last_success or last_error >= last_success):
        return "Fora de Operação", "#cc1f1a"  # vermelho
    return "Indeterminado", "#6b7280"  # cinza

def format_error_list(hb: dict) -> list[str]:
    out = []
    last_error = hb.get("last_error", "")
    last_error_msg = hb.get("last_error_msg", "")
    if last_error:
        item = f"{last_error} - {last_error_msg}" if last_error_msg else f"{last_error}"
        out.append(item)
    return out

# ----------------------------------
# Envio de e-mail (thread)
# ----------------------------------
def threaded_resend_email(button: ttk.Button, root: tk.Tk, sheet_path: str):
    def _task():
        try:
            button.state(["disabled"])
            from mailer import send_weekly_email
            send_weekly_email(sheet_path)
            messagebox.showinfo("Reenvio de E-mails", "E-mail reenviado com sucesso!")
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("Erro ao reenviar", f"Ocorreu um erro ao reenviar o e-mail:\n{e}")
        finally:
            try:
                button.state(["!disabled"])
            except Exception:
                pass

    t = threading.Thread(target=_task, daemon=True)
    t.start()

# ----------------------------------
# GUI
# ----------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Acompanhamento – Diesel & Petróleo")
        self.geometry("760x820")
        self.minsize(640, 760)

        # Estilo
        try:
            self.call("tk", "scaling", 1.25)
        except Exception:
            pass
        style = ttk.Style(self)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        style.configure("TLabel", font=("Segoe UI", 11))
        style.configure("Title.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Status.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Footer.TLabel", font=("Segoe UI", 10, "italic"), foreground="#6b7280")
        style.configure("Card.TLabelframe.Label", font=("Segoe UI", 12, "bold"))

        # Container principal
        wrap = ttk.Frame(self, padding=14)
        wrap.pack(fill="both", expand=True)

        # --------- LOGO SUPERIOR ----------
        self.frame_logo_top = ttk.Labelframe(wrap, text="  ", padding=12, style="Card.TLabelframe")
        self.frame_logo_top.pack(fill="both", expand=False, pady=(0, 10))
        self.logo1_label = ttk.Label(self.frame_logo_top)
        self.logo1_label.pack(expand=True)
        self._logo1_img_ref = None

        # --------- STATUS ----------
        self.frame_status = ttk.Labelframe(wrap, text="Status do Sistema", padding=12, style="Card.TLabelframe")
        self.frame_status.pack(fill="x", pady=(0, 10))

        self.lbl_status_title = ttk.Label(self.frame_status, text="Status:", style="Title.TLabel")
        self.lbl_status_title.pack(anchor="w")

        self.lbl_status_value = ttk.Label(self.frame_status, text="—", style="Status.TLabel")
        self.lbl_status_value.pack(anchor="w", pady=(6, 0))

        self.lbl_last_run = ttk.Label(self.frame_status, text="Última execução: —")
        self.lbl_last_run.pack(anchor="w", pady=(6, 0))

        # --------- ERROS ----------
        self.frame_erros = ttk.Labelframe(wrap, text="Dias registrados com erro", padding=12, style="Card.TLabelframe")
        self.frame_erros.pack(fill="both", expand=False, pady=(0, 10))

        self.err_listbox = tk.Listbox(self.frame_erros, height=4, font=("Consolas", 11))
        self.err_listbox.pack(fill="both", expand=True)

        # --------- BOTÃO ----------
        self.btn_resend = ttk.Button(
            wrap, text="Reenviar E-Mails",
            command=lambda: threaded_resend_email(self.btn_resend, self, SHEET_PATH)
        )
        self.btn_resend.pack(pady=(0, 10))

        # --------- LOGO INFERIOR ----------
        self.frame_logo_bottom = ttk.Labelframe(wrap, text="  ", padding=12, style="Card.TLabelframe")
        self.frame_logo_bottom.pack(fill="both", expand=True, pady=(0, 10))
        self.logo2_label = ttk.Label(self.frame_logo_bottom)
        self.logo2_label.pack(expand=True)
        self._logo2_img_ref = None

        # --------- RODAPÉ ----------
        self.footer = ttk.Label(wrap, text="dev: Thiagoscocco", style="Footer.TLabel")
        self.footer.pack(side="bottom", anchor="e")

        # Carrega logos e inicia atualizações
        self.load_logos()
        self.refresh_status()
        self.after(REFRESH_MS, self._auto_refresh)

    # ------ Métodos de atualização ------
    def _load_logo(self, path: str, target_label: ttk.Label, store_attr: str):
        if not os.path.exists(path):
            target_label.configure(text=f"(Logo não encontrada em {path})")
            setattr(self, store_attr, None)
            return
        try:
            if PIL_OK:
                # Redimensiona ambas para o MESMO tamanho máximo
                max_w, max_h = 310, 160
                img = Image.open(path)
                img.thumbnail((max_w, max_h))
                ref = ImageTk.PhotoImage(img)
            else:
                ref = tk.PhotoImage(file=path)  # sem redimensionamento
            target_label.configure(image=ref, text="")
            setattr(self, store_attr, ref)  # manter referência
        except Exception as e:
            target_label.configure(text=f"Erro ao carregar logo: {e}")
            setattr(self, store_attr, None)

    def load_logos(self):
        self._load_logo(LOGO1_PATH, self.logo1_label, "_logo1_img_ref")
        self._load_logo(LOGO2_PATH, self.logo2_label, "_logo2_img_ref")

    def refresh_status(self):
        hb = read_heartbeat()
        status_txt, status_color = status_from_heartbeat(hb)

        self.lbl_status_value.configure(text=status_txt, foreground=status_color)

        last_run = hb.get("last_run", "—")
        try:
            if last_run and last_run != "—":
                dt = datetime.fromisoformat(last_run)
                last_run_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                last_run_fmt = "—"
        except Exception:
            last_run_fmt = last_run
        self.lbl_last_run.configure(text=f"Última execução: {last_run_fmt}")

        self.err_listbox.delete(0, tk.END)
        for item in format_error_list(hb):
            self.err_listbox.insert(tk.END, item)

    def _auto_refresh(self):
        self.refresh_status()
        self.after(REFRESH_MS, self._auto_refresh)

if __name__ == "__main__":
    app = App()
    app.mainloop()
