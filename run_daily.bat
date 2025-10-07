@echo off
setlocal
chcp 65001 >nul

REM === Caminho do seu projeto ===
set "PROJECT_DIR=C:\Users\agrot\Desktop\Arquivos\Thiago\PYTHON\Programas em Desenvolvimento\Diesel Petróleo Var\software_diesel_petroleo"

REM === (Opcional) Se você quiser fixar o Python exato, preencha PYTHON_EXE ===
REM set "PYTHON_EXE=C:\Users\SEU_USUARIO\AppData\Local\Programs\Python\Python312\python.exe"

REM === Use o Python Launcher (recomendado) se PYTHON_EXE não estiver definido ===
if not defined PYTHON_EXE set "PYTHON_EXE=py -3"

REM === Ir para a pasta do projeto ===
cd /d "%PROJECT_DIR%"

REM === Garantir pastas usadas pelo app ===
if not exist "runtime" mkdir "runtime"
if not exist "logs" mkdir "logs"

echo ============================================== >> "logs\run.log"
echo [%date% %time%] Iniciando coleta >> "logs\run.log"

REM === (Opcional) ativar venv se você tiver um: venv\Scripts\activate ===
REM if exist "venv\Scripts\activate.bat" (
REM     call "venv\Scripts\activate.bat"
REM )

REM === Executar o script principal ===
%PYTHON_EXE% main.py >> "logs\run.log" 2>&1

set "ERR=%ERRORLEVEL%"
if "%ERR%"=="0" (
  echo [%date% %time%] Execução concluída com sucesso. >> "logs\run.log"
) else (
  echo [%date% %time%] ERRO! Código %ERR% >> "logs\run.log"
)

exit /b %ERR%
