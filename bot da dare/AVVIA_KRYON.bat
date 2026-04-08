@echo off
TITLE Kryon Master Quant - Auto Launcher
color 0B

echo ========================================================
echo         KRYON QUANT HFT - INIZIALIZZAZIONE SISTEMA
echo ========================================================
echo.

:: 1. Controllo presenza Python
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    color 0C
    echo [X] ALLARME: Python non e' installato su questo PC!
    echo Per far girare il bot, scarica Python da python.org
    echo IMPORTANTE: Durante l'installazione spunta la casella "Add Python to PATH"!
    echo.
    pause
    exit
)

echo [V] Python rilevato correttamente.
echo.
echo [!] Sincronizzazione dell'Arsenale Quantitativo in corso...
echo     (La prima volta potrebbe richiedere un minuto, porta pazienza)
echo.

:: 2. Installazione/Aggiornamento automatico e silenzioso delle librerie
pip install MetaTrader5 pandas numpy scikit-learn matplotlib Pillow --quiet

echo.
echo [V] Tutte le librerie neurali e matematiche sono pronte e aggiornate!
echo.
echo [!] AVVIO MOTORE KRYON IN CORSO...
echo.

:: 3. Avvio del bot di nascosto senza lasciare la finestra nera aperta
start pythonw kryon.pyw

:: 4. Chiusura del launcher
exit