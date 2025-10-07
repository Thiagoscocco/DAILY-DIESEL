# customtkinter-based minimal UI for email sending and running consulta
import os
import threading
import customtkinter as ctk
from tkinter import messagebox
from dotenv import load_dotenv

load_dotenv()

SHEET_PATH = os.getenv("SHEET_PATH", "data/planilha_unica.xlsx").strip()

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Daily Diesel")
        self.geometry("560x420")
        self.minsize(520, 360)

        self.recipients: list[str] = []

        # Layout principal
        self.grid_columnconfigure(0, weight=1)

        header = ctk.CTkLabel(self, text="ENVIO SEMANAL", font=("Segoe UI", 20, "bold"))
        header.grid(row=0, column=0, pady=(16, 8), padx=16, sticky="n")

        # Caixa de emails
        frame_emails = ctk.CTkFrame(self)
        frame_emails.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 12))
        frame_emails.grid_columnconfigure(1, weight=1)

        lbl = ctk.CTkLabel(frame_emails, text="E-mails (um por vez):")
        lbl.grid(row=0, column=0, padx=12, pady=12, sticky="w")

        self.entry_email = ctk.CTkEntry(frame_emails, placeholder_text="ex: pessoa@empresa.com")
        self.entry_email.grid(row=0, column=1, padx=(0, 12), pady=12, sticky="ew")

        self.btn_add = ctk.CTkButton(frame_emails, text="ADICIONAR", command=self._add_email)
        self.btn_add.grid(row=0, column=2, padx=12, pady=12)

        self.listbox = ctk.CTkTextbox(frame_emails, height=120)
        self.listbox.grid(row=1, column=0, columnspan=3, padx=12, pady=(0, 12), sticky="nsew")
        self.listbox.configure(state="disabled")

        # Ações
        actions = ctk.CTkFrame(self)
        actions.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 12))
        actions.grid_columnconfigure((0, 1), weight=1)

        self.btn_send = ctk.CTkButton(actions, text="ENVIAR E-MAILS", command=self._send_emails)
        self.btn_send.grid(row=0, column=0, padx=(0, 6), pady=6, sticky="ew")

        self.btn_run = ctk.CTkButton(actions, text="RODAR CONSULTA", command=self._run_consulta)
        self.btn_run.grid(row=0, column=1, padx=(6, 0), pady=6, sticky="ew")

        # Rodapé simples
        footer = ctk.CTkFrame(self, fg_color="transparent")
        footer.grid(row=3, column=0, padx=16, pady=(8, 16), sticky="ew")
        footer.grid_columnconfigure(0, weight=1)
        footer.grid_columnconfigure(1, weight=0)

        self.status = ctk.CTkLabel(footer, text="Pronto.")
        self.status.grid(row=0, column=0, sticky="w")

        self.dev = ctk.CTkLabel(footer, text="DEV:Thiagoscocco", text_color="#6b7280")
        self.dev.grid(row=0, column=1, sticky="e")

    def _set_status(self, text: str) -> None:
        self.status.configure(text=text)

    def _add_email(self) -> None:
        email = (self.entry_email.get() or "").strip()
        if not email:
            messagebox.showwarning("E-mail vazio", "Digite um e-mail válido.")
            return
        self.recipients.append(email)
        self.entry_email.delete(0, "end")
        self._refresh_list()

    def _refresh_list(self) -> None:
        self.listbox.configure(state="normal")
        self.listbox.delete("1.0", "end")
        for e in self.recipients:
            self.listbox.insert("end", f"{e}\n")
        self.listbox.configure(state="disabled")

    def _send_emails(self) -> None:
        def worker():
            try:
                self.btn_send.configure(state="disabled")
                from mailer import send_weekly_email
                send_weekly_email(SHEET_PATH, recipients=self.recipients or None)
                messagebox.showinfo("E-mail", "E-mails enviados com sucesso!")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao enviar e-mails:\n{e}")
            finally:
                self.btn_send.configure(state="normal")
                self._set_status("Pronto.")

        self._set_status("Enviando…")
        threading.Thread(target=worker, daemon=True).start()

    def _run_consulta(self) -> None:
        def worker():
            try:
                self.btn_run.configure(state="disabled")
                from main import run_consulta
                ref_date = run_consulta(send_email_if_day=False)
                messagebox.showinfo("Consulta", f"Consulta concluída para {ref_date}.")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha na consulta:\n{e}")
            finally:
                self.btn_run.configure(state="normal")
                self._set_status("Pronto.")

        self._set_status("Consultando…")
        threading.Thread(target=worker, daemon=True).start()


if __name__ == "__main__":
    app = App()
    app.mainloop()
