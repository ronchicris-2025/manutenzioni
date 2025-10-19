import streamlit as st
import pandas as pd
import sqlite3
import os
import datetime
from dateutil.relativedelta import relativedelta
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import time
import uuid
import folium
from streamlit_folium import st_folium
from streamlit_pdf_viewer import pdf_viewer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import re
import io 
from io import BytesIO
import requests
import base64
from github import Github, GithubException
import tempfile

# --- CONFIGURAZIONE E COSTANTI ---
st.set_page_config(
    page_title="Gestione Manutenzioni",
    page_icon="🔧",
    layout="wide"
)

DB_FILE = "manutenzioni.db"

# FUNZIONE PER STREAMLIT CLOUD : RIPRISTINA I FILE .DB DA GITHUB 

# --- Funzioni di backup/restore ---
def get_github_repo():
    token = st.secrets["github"]["token"]
    repo_name = st.secrets["github"]["repo"]
    branch = st.secrets["github"]["branch"]
    g = Github(token)
    repo = g.get_repo(repo_name)
    return repo, branch


def backup_to_github_simple():
    github_token = st.secrets["github"]["token"]
    repo = st.secrets["github"]["repo"]
    branch = st.secrets["github"]["branch"]

    db_files = ["login_log.db", "manutenzioni.db"]
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github+json"
    }

    for db_file in db_files:
        if os.path.exists(db_file):
            try:
                with open(db_file, "rb") as f:
                    content = base64.b64encode(f.read()).decode("utf-8")

                # Controlla SHA esistente per evitare errori 409
                get_url = f"https://api.github.com/repos/{repo}/contents/{db_file}?ref={branch}"
                get_res = requests.get(get_url, headers=headers)
                sha = get_res.json().get("sha") if get_res.status_code == 200 else None

                # Effettua il PUT
                put_url = f"https://api.github.com/repos/{repo}/contents/{db_file}"
                data = {
                    "message": f"💾 Backup {db_file} da Streamlit",
                    "content": content,
                    "branch": branch,
                }
                if sha:
                    data["sha"] = sha

                put_res = requests.put(put_url, headers=headers, json=data)
                if put_res.status_code in [200, 201]:
                    st.success(f"✅ {db_file} salvato su GitHub")
                else:
                    st.error(f"❌ Errore salvando {db_file}: {put_res.status_code}")
                    st.text(put_res.text)
            except Exception as e:
                st.error(f"❌ Errore durante backup di {db_file}: {e}")
        else:
            st.warning(f"⚠️ File {db_file} non trovato in locale — nessun backup eseguito.")

# === RESTORE FROM GITHUB===

def restore_from_github_simple():
    """
    🔄 Ripristina i database da GitHub *solo se non presenti in locale*.
    Usa i parametri di connessione da st.secrets["github"].
    """
    try:
        github_conf = st.secrets["github"]
        token = github_conf["token"]
        repo = github_conf["repo"]
        branch = github_conf.get("branch", "main")

        db_files = ["manutenzioni.db", "login_log.db"]
        headers = {"Authorization": f"token {token}"}

        restored = []

        for db_file in db_files:
            if os.path.exists(db_file):
                st.info(f"✅ {db_file} già presente in locale, nessun download necessario.")
                continue

            # URL API GitHub per ottenere il file
            url = f"https://api.github.com/repos/{repo}/contents/{db_file}?ref={branch}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                content_json = response.json()
                encoded_content = content_json.get("content", "")
                encoding = content_json.get("encoding", "")

                if encoding == "base64" and encoded_content:
                    # Decodifica e salva localmente
                    binary_data = base64.b64decode(encoded_content)
                    with open(db_file, "wb") as f:
                        f.write(binary_data)
                    restored.append(db_file)
                    st.success(f"✅ {db_file} ripristinato da GitHub.")
                else:
                    st.warning(f"⚠️ {db_file} trovato ma encoding non valido ({encoding}).")
            elif response.status_code == 404:
                st.warning(f"⚠️ {db_file} non trovato nel repo GitHub.")
            else:
                st.error(f"❌ Errore GitHub ({response.status_code}): {response.text}")

        if restored:
            st.info(f"✅ Database ripristinati: {', '.join(restored)}")
        else:
            st.warning("⚠️ Nessun database ripristinato (già presenti o non trovati).")

    except KeyError:
        st.error("❌ Errore: credenziali GitHub non trovate in st.secrets['github'].")
    except Exception as e:
        st.error(f"❌ Errore imprevisto durante il ripristino: {e}")



## FUNZIONI PER SALVATAGGIO E VISUALIZZAZIONE INFO BACKUP TO GITHUB  

BACKUP_TIMESTAMP_FILE = "last_backup_time.txt"


def save_backup_timestamp():
    """Salva localmente l’orario dell’ultimo backup."""
    with open(BACKUP_TIMESTAMP_FILE, "w") as f:
        f.write(datetime.datetime.now().isoformat())


def get_backup_timestamp():
    """Restituisce la data e ora dell’ultimo backup (formattata)."""
    if os.path.exists(BACKUP_TIMESTAMP_FILE):
        with open(BACKUP_TIMESTAMP_FILE, "r") as f:
            timestamp = f.read().strip()
            try:
                dt = datetime.datetime.fromisoformat(timestamp)
                return dt.strftime("%d/%m/%Y %H:%M:%S")
            except Exception:
                return None
    return None
# VERIFICA SE CI SONO LE CREDENZIALI E SE IL REPO è VISTO CORRETTAMENTE SU GITHUB
def test_github_connection():
    try:
        github_token = st.secrets["github"]["token"]
        repo = st.secrets["github"]["repo"]
        branch = st.secrets["github"]["branch"]

        url = f"https://api.github.com/repos/{repo}/branches/{branch}"
        headers = {"Authorization": f"token {github_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            st.success(f"✅ Connessione a GitHub OK. Branch '{branch}' trovato nel repo '{repo}'.")
        elif response.status_code == 404:
            st.error(f"⚠️ Branch '{branch}' o repository '{repo}' non trovato su GitHub (404).")
        elif response.status_code == 401:
            st.error("❌ Token GitHub non valido o senza permessi.")
        else:
            st.warning(f"⚠️ Risposta inattesa da GitHub: {response.status_code} - {response.text}")

    except KeyError as e:
        st.error(f"❌ Chiave mancante in st.secrets: {e}")



def test_github_db_files():
    github_token = st.secrets["github"]["token"]
    repo_name = st.secrets["github"]["repo"]
    branch_name = st.secrets["github"]["branch"]

    try:
        g = Github(github_token)
        repo = g.get_repo(repo_name)
        st.info(f"✅ Connessione a GitHub OK. Repo '{repo_name}' trovata.")

        # Verifica branch
        branch = repo.get_branch(branch_name)
        st.info(f"✅ Branch '{branch_name}' trovato nel repo.")

        # Controlla file login_log.db
        try:
            login_log_file = repo.get_contents("login_log.db", ref=branch_name)
            st.success(f"✅ File 'login_log.db' trovato su GitHub (sha: {login_log_file.sha})")
        except Exception as e:
            st.warning(f"⚠️ File 'login_log.db' non trovato su GitHub: {e}")

        # Controlla file manutenzioni.db
        try:
            manut_file = repo.get_contents("manutenzioni.db", ref=branch_name)
            st.success(f"✅ File 'manutenzioni.db' trovato su GitHub (sha: {manut_file.sha})")
        except Exception as e:
            st.warning(f"⚠️ File 'manutenzioni.db' non trovato su GitHub: {e}")

    except Exception as e:
        st.error(f"❌ Errore connessione GitHub: {e}")


# FUNZIONE INIZIALIZZAZIONE LOGIN

def init_login_log():
    """
    Crea il database login_log.db e la tabella login_log
    se non esistono ancora.
    """
    conn = sqlite3.connect("login_log.db")  # file nella cartella principale
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS login_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            role TEXT,
            login_time TEXT NOT NULL,
            logout_time TEXT,
            session_duration REAL,
            success INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()

# Chiama all'avvio
#init_login_log()

# --------------------------
# 2️⃣ Log dei tentativi di login
# --------------------------
def log_login_attempt(username, success, ip=None):
    conn = sqlite3.connect("login_log.db")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO login_log (username, login_time, success, ip) VALUES (?, ?, ?, ?)",
        (username, datetime.datetime.now().isoformat(), int(success), ip)
    )
    conn.commit()
    conn.close()

# --------------------------
# 3️⃣ Funzione di login principale
# --------------------------

def check_login():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
        st.session_state["username"] = None
        st.session_state["role"] = None
        st.session_state["login_start_time"] = None

    # --- FORM LOGIN ---
    if not st.session_state["logged_in"]:
        st.subheader("🔐 Login richiesto")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Accedi"):
            if "users" not in st.secrets:
                st.error("❌ Nessuna configurazione utenti trovata in `secrets.toml`.")
                st.stop()

            users = st.secrets["users"]

            # Controllo credenziali
            if username in users and password == users[username]["password"]:
                role = users[username].get("role", "user")
                st.session_state["logged_in"] = True
                st.session_state["username"] = username
                st.session_state["role"] = role
                st.session_state["login_start_time"] = datetime.datetime.now()

                # 🔹 Log login
                conn = sqlite3.connect("login_log.db")
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO login_log (username, role, login_time, success)
                    VALUES (?, ?, ?, ?)
                """, (username, role, st.session_state["login_start_time"].isoformat(), 1))
                conn.commit()
                conn.close()

                st.success(f"✅ Accesso eseguito come **{username}** ({role})")
                st.rerun()
            else:
                st.error("❌ Username o password errati.")
                # 🔹 Log tentativo fallito
                conn = sqlite3.connect("login_log.db")
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO login_log (username, role, login_time, success)
                    VALUES (?, ?, ?, ?)
                """, (username, None, datetime.datetime.now().isoformat(), 0))
                conn.commit()
                conn.close()
        st.stop()

    # --- PANNELLO UTENTE LOGGATO ---
    else:
        col1, col2 = st.columns([4, 1])
        with col1:
            st.info(f"👤 Utente: **{st.session_state['username']}** | Ruolo: **{st.session_state['role']}**")
        with col2:
            if st.button("🚪 Logout"):
                logout_time = datetime.datetime.now()
                session_start = st.session_state.get("login_start_time")
                if session_start:
                    duration_min = (logout_time - session_start).total_seconds() / 60.0  # durata in minuti

                    # 🔹 Aggiorna l'ultimo log di login
                    conn = sqlite3.connect("login_log.db")
                    cursor = conn.cursor()

                # Trova l'ultimo login aperto
                    cursor.execute("""
                        SELECT id FROM login_log
                        WHERE username = ? AND logout_time IS NULL
                        ORDER BY login_time DESC
                        LIMIT 1
                    """, (st.session_state["username"],))
                    row = cursor.fetchone()

                    if row:
                        last_id = row[0]
                        cursor.execute("""
                        UPDATE login_log
                        SET logout_time = ?, session_duration = ?
                        WHERE id = ?
                        """, (logout_time.isoformat(), duration_min, last_id))

                    conn.commit()
                    conn.close()


                # Resetta sessione
                st.session_state["logged_in"] = False
                st.session_state["username"] = None
                st.session_state["role"] = None
                st.session_state["login_start_time"] = None
                st.rerun()



# --------------------------
# 4️⃣ Pannello storico login filtrabile
# --------------------------
def show_login_history():
    st.header("📜 Storico Accessi Utenti")

    conn = sqlite3.connect("login_log.db")
    df = pd.read_sql_query("SELECT * FROM login_log ORDER BY login_time DESC", conn)
    conn.close()

    if df.empty:
        st.info("Nessun accesso registrato finora.")
        return

    df["login_time"] = pd.to_datetime(df["login_time"], errors="coerce", format="mixed")
    if "logout_time" in df.columns:
        df["logout_time"] = pd.to_datetime(df["logout_time"], errors="coerce", format="mixed")

    # --- FILTRI ---
    with st.expander("🔍 Filtra i risultati", expanded=True):
        col1, col2 = st.columns(2)
        utenti = ["Tutti"] + sorted(df["username"].dropna().unique().tolist())
        selected_user = col1.selectbox("👤 Utente", utenti)
        start_date = col2.date_input("📅 Da data", value=df["login_time"].min().date())
        end_date = col2.date_input("📅 A data", value=df["login_time"].max().date())

    filtered_df = df.copy()
    if selected_user != "Tutti":
        filtered_df = filtered_df[filtered_df["username"] == selected_user]
    filtered_df = filtered_df[
        (filtered_df["login_time"].dt.date >= start_date) &
        (filtered_df["login_time"].dt.date <= end_date)
    ]

    st.dataframe(filtered_df, use_container_width=True)

    # --- Esporta CSV ---
    csv = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Esporta Log in CSV", data=csv,
                       file_name=f"log_accessi_{datetime.date.today()}.csv", mime='text/csv')

    # --- Pulsante per svuotare log ---
    if st.session_state.get("role") == "admin":
        st.markdown("---")
        if st.button("🧹 Svuota completamente il log accessi", type="secondary"):
            with st.spinner("Pulizia in corso..."):
                conn = sqlite3.connect("login_log.db")
                conn.execute("DELETE FROM login_log")
                conn.commit()
                conn.close()
                st.success("✅ Log accessi completamente svuotato!")
                st.rerun()



# Colonne per la tabella 'manutenzioni'
MANUTENZIONI_COLUMNS = [
    "punto_vendita", "indirizzo", "cap", "citta", "provincia", "regione",
    "ultimo_intervento", "prossimo_intervento", "attrezzature", "note", 
    "lat", "lon", "codice", "brand", "referente_pv", "telefono"
]

# Colonne per la tabella 'comuni' da Excel
COMUNI_COLUMNS = [
    "codice", "comune", "cap", "provincia", "regione", "codice2", "lat", "lon", "extra"
]

# --- FUNZIONI HELPER (MODEL) ---

def get_connection():
    """Ottiene una connessione al database."""
    return sqlite3.connect(DB_FILE)

def init_db():
    """Inizializza il database e crea le tabelle se non esistono."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # --- TABELLA MANUTENZIONI (rimane uguale) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS manutenzioni (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            punto_vendita TEXT NOT NULL,
            indirizzo TEXT NOT NULL,
            cap TEXT,
            citta TEXT NOT NULL,
            provincia TEXT,
            regione TEXT,
            ultimo_intervento DATE,
            prossimo_intervento DATE,
            attrezzature TEXT,
            note TEXT,
            lat REAL,
            lon REAL,
            codice TEXT,
            brand TEXT,
            referente_pv TEXT,  
            telefono TEXT        
        )
    ''')

    # --- TABELLA COMUNI (rimane uguale) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comuni (
            comune TEXT PRIMARY KEY,
            codice TEXT,
            cap TEXT,
            provincia TEXT,
            regione TEXT,
            codice2 TEXT,
            lat REAL,
            lon REAL,
            extra TEXT
        )
    ''')

    # --- TABELLA FORMAT (rimane uguale) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS format (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT UNIQUE NOT NULL
        )
    ''')
    
    
    # --- TABELLA PROGRAMMAZIONE (VERSIONE CON AGGIORNAMENTO) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS programmazione (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_order_id TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            punto_vendita TEXT NOT NULL,
            indirizzo TEXT,
            cap TEXT,
            citta TEXT,
            provincia TEXT,
            tecnico_assegnato TEXT,
            data_programmata DATE,
            distanza_totale REAL,
            referente_pv TEXT,   
            telefono TEXT, 
            note TEXT,       
            orario_previsto TIME   
        )
    ''')
    # --- TABELLA STORICO PROGRAMMAZIONE (VERSIONE CON AGGIORNAMENTO) ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS storico_prog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_order_id TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            work_order_number INTEGER,
            punto_vendita TEXT NOT NULL,
            indirizzo TEXT,
            cap TEXT,
            citta TEXT,
            provincia TEXT,
            tecnico_assegnato TEXT,
            data_programmata DATE,
            distanza_totale REAL,
            referente_pv TEXT,
            telefono TEXT,
            note TEXT,
            orario_previsto TIME
        )
    ''')
    
    try:
        cursor.execute("ALTER TABLE manutenzioni ADD COLUMN referente_pv TEXT")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE manutenzioni ADD COLUMN telefono TEXT")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE programmazione ADD COLUMN work_order_number INTEGER")
    except sqlite3.OperationalError:pass
    try:
        cursor.execute("ALTER TABLE programmazione ADD COLUMN referente_pv TEXT")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE programmazione ADD COLUMN telefono TEXT")
    except sqlite3.OperationalError: pass
    try:
        cursor.execute("ALTER TABLE programmazione ADD COLUMN orario_previsto TIME")
    except sqlite3.OperationalError: pass
    
    try:
        cursor.execute("CREATE VIEW IF NOT EXISTS storico_manutenzioni AS SELECT * FROM manutenzioni")
    except sqlite3.OperationalError: pass
    
    conn.commit()
    conn.close()

def load_data(table_name="manutenzioni"):
    """Carica i dati da una tabella specifica in un DataFrame di pandas."""
    conn = get_connection()
    try:
        if table_name == "manutenzioni":
            df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY ID DESC", conn)
            if not df.empty:
                df['ultimo_intervento'] = pd.to_datetime(df['ultimo_intervento'], errors='coerce').dt.date
                df['prossimo_intervento'] = pd.to_datetime(df['prossimo_intervento'], errors='coerce').dt.date
        elif table_name == "comuni":
            df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY comune ASC", conn)
        elif table_name == "format":
            df = pd.read_sql_query(f"SELECT brand FROM {table_name} ORDER BY brand ASC", conn)
        return df
    finally:
        conn.close()

import streamlit as st
import pandas as pd

def show_pending_notice():
    """
    Mostra un avviso 🔔 cliccabile nella sidebar se ci sono ordini pendenti in 'programmazione'.
    Quando cliccato, porta automaticamente alla pagina '📋 Ordini Attivi'.
    """
    try:
        conn = get_connection()
        df_prog = pd.read_sql_query("SELECT COUNT(*) AS count FROM programmazione", conn)
        conn.close()

        pending_count = int(df_prog.iloc[0]['count'])

        if pending_count > 0:
            st.sidebar.markdown(
                f"""
                <div style="
                    background-color:#fff8e1;
                    border-left:5px solid #f1c40f;
                    padding:12px;
                    border-radius:10px;
                    margin-bottom:10px;
                    cursor:pointer;
                    transition: background-color 0.3s;
                "
                onmouseover="this.style.backgroundColor='#ffecb3';"
                onmouseout="this.style.backgroundColor='#fff8e1';"
                onclick="window.location.href='?page=ordini_attivi';"
                >
                    <span style="font-size:18px;">🔔 <b>{pending_count}</b> ordine/i pendente/i</span><br>
                    <span style="font-size:13px; color:#666;">Clicca per gestire gli ordini attivi</span>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.sidebar.markdown(
                """
                <div style="
                    background-color:#e8f5e9;
                    border-left:5px solid #2ecc71;
                    padding:10px;
                    border-radius:10px;
                    margin-bottom:10px;
                    font-size:14px;
                ">
                    ✅ Tutti gli ordini risultano completati
                </div>
                """,
                unsafe_allow_html=True
            )

    except Exception as e:
        st.sidebar.error(f"Errore nel controllo ordini pendenti: {e}")


def save_manutenzione(edited_df, original_df):
    """
    Confronta il DataFrame modificato con quello originale e aggiorna il database.
    Usa iterrows() per maggiore robustezza e per evitare problemi di tipo.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # --- 1. CANCELLAZIONE: Trova e rimuovi le righe eliminate dall'editor ---
        deleted_ids = set(original_df['ID']) - set(edited_df['ID'])
        for id_to_delete in deleted_ids:
            cursor.execute("DELETE FROM manutenzioni WHERE ID = ?", (id_to_delete,))

        # --- 2. INSERIMENTO: Trova e aggiungi le nuove righe ---
        new_rows = edited_df[edited_df['ID'].isnull()]
        if not new_rows.empty:
            for index, row in new_rows.iterrows():
                # Rimuove la colonna ID per l'inserimento
                row_to_insert = row.drop('ID')
                cursor.execute(f'''
                    INSERT INTO manutenzioni ({", ".join(MANUTENZIONI_COLUMNS)})
                    VALUES ({", ".join(["?"]*len(MANUTENZIONI_COLUMNS))})
                ''', tuple(row_to_insert))

        # --- 3. AGGIORNAMENTO: Trova e aggiorna le righe modificate ---
        # Confronta i DataFrame uniti per trovare le differenze
        merged_df = pd.merge(edited_df, original_df, on='ID', suffixes=('_new', '_old'))
        updated_rows = merged_df[merged_df['ID'].notnull()]
        
        if not updated_rows.empty:
            # --- CORREZIONE CHIAVE: Usa un ciclo standard e più sicuro ---
            for index, row in updated_rows.iterrows():
                update_columns = []
                update_values = []
                
                # Itera su tutte le colonne dati per trovare i cambiamenti
                for col in MANUTENZIONI_COLUMNS:
                    # Confronta il valore nuovo con quello vecchio, gestendo i NaN
                    val_new = row[f'{col}_new']
                    val_old = row[f'{col}_old']
                    if pd.isna(val_new) and pd.isna(val_old):
                        continue # Se entrambi sono NaN, non c'è cambiamento
                    if val_new != val_old:
                        update_columns.append(f"{col} = ?")
                        update_values.append(val_new)
                
                # Se ci sono colonne da aggiornare, esegui la query
                if update_columns:
                    set_clause = ", ".join(update_columns)
                    update_values.append(row['ID'])
                    query = f"UPDATE manutenzioni SET {set_clause} WHERE ID = ?"
                    cursor.execute(query, update_values)

        conn.commit()
        st.success("Modifiche salvate con successo!")
        return True

    except Exception as e:
        conn.rollback()
        st.error(f"Errore durante il salvataggio: {e}")
        return False
    finally:
        conn.close()
        
# FUNZIONE GLOBALE SALVATAGGI
       
def save_and_rerun(df_to_save):
    """Salva le modifiche e aggiorna la pagina."""
    try:
        st.session_state['edited_df'] = df_to_save
        st.success("Modifiche salvate con successo!")
        st.rerun()
    except Exception as e:
        st.error(f"Errore durante il salvataggio: {e}")
        
def save_and_run(df_to_save=None):
    """Funzione generica per salvare e aggiornare lo stato della sessione."""
    try:
        st.session_state['edited_df'] = df_to_save
        st.success("Dati salvati con successo!")
        st.rerun()
    except Exception as e:
        st.error(f"Errore durante il salvataggio: {e}")

        
# --- FUNZIONI DELLE PAGINE PRINCIPALI (VIEW) ---

def show_gestione_manutenzioni():
    st.header("🏛 Gestione Punti Vendita")
    
    if st.session_state.get("reset_form_flag", False):
        for key in st.session_state.keys():
            if key.endswith("_form"):
                del st.session_state[key]
        st.session_state.citta_select_reactive = ""
        st.session_state.reset_form_flag = False
        st.rerun()

    df_brands = load_data("format")
    brand_list = df_brands['brand'].tolist()
    
    tab1, tab2 = st.tabs(["📊 Tabella PV ", "➕ Aggiungi Punto Vendita "])
    
    with tab1:
        selected_brand_filter = st.selectbox("Filtra per Brand/Formato: (es. CONDAD, CARREFOUR IPER; CARREFOUR MARKET)", options=["Tutti"] + brand_list)
        
        
        df_manutenzioni = load_data("manutenzioni")
        if selected_brand_filter != "Tutti":
            df_manutenzioni = df_manutenzioni[df_manutenzioni['brand'] == selected_brand_filter]
        df_to_show = df_manutenzioni.copy()
        df_to_show.insert(0, "Seleziona", False)
         
            
        ## DATA EDITOR
        edited_df = st.data_editor(
            df_to_show,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "ID": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "Seleziona": st.column_config.CheckboxColumn("Seleziona", help="Spunta per cancellare questo record"),
                "ultimo_intervento": st.column_config.DateColumn("Ultimo Intervento"),
                "prossimo_intervento": st.column_config.DateColumn("Prossimo Intervento"),
                "lat": st.column_config.NumberColumn("Latitudine", format="%.6f"),
                "lon": st.column_config.NumberColumn("Longitudine", format="%.6f"),
                "referente_pv": st.column_config.TextColumn("Referente PV"),
                "telefono": st.column_config.TextColumn("Telefono"),
            },
            disabled=["ID"]
        )

        selected_rows = edited_df[edited_df["Seleziona"] == True]
        ids_to_delete = selected_rows['ID'].tolist()

        if ids_to_delete:
            with st.expander("⚠️ Conferma Cancellazione", expanded=True):
                st.warning(f"Stai per cancellare {len(ids_to_delete)} record. Questa operazione è irreversibile.")
                if st.button("Sì, cancella definitivamente", type="primary"):
                    conn = get_connection()
                    cursor = conn.cursor()
                    try:
                        placeholders = ','.join(['?'] * len(ids_to_delete))
                        query = f"DELETE FROM manutenzioni WHERE ID IN ({placeholders})"
                        cursor.execute(query, ids_to_delete)
                        conn.commit()
                        st.success(f"{len(ids_to_delete)} record cancellati con successo!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Errore durante la cancellazione: {e}")
                    finally:
                        conn.close()

        if st.button("💾 Salva Modifiche", type="primary"):
            df_to_save = edited_df.drop('Seleziona', axis=1)
            df_to_save = df_to_save[~df_to_save['ID'].isin(ids_to_delete)]
            
            if not df_to_save.equals(df_manutenzioni):
                if save_manutenzione(df_to_save, df_manutenzioni):
                    st.rerun()
            else:
                st.info("Nessuna modifica da salvare.")
        # Pulsante per scaricare Excel
        if st.button("📥 Scarica Excel tabella manutenzioni"):
            output = io.BytesIO()
            df_to_export = df_manutenzioni.copy()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_to_export.to_excel(writer, index=False, sheet_name='Manutenzioni')
            st.download_button(
                label="Download Excel",
                data=output.getvalue(),
                file_name=f"manutenzioni_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )  
    with tab2:
        st.subheader("Inserisci un nuovo punto vendita")
    
        df_comuni = load_data("comuni")
        comuni_list = df_comuni['comune'].tolist()
    
        selected_comune = st.selectbox(
            "Seleziona Città *", 
            options=[""] + comuni_list,
            key="citta_select_reactive",
            help="Selezionando una città, i campi sottostanti verranno compilati automaticamente."
        )
    
        # Inizializza campi auto se non esistono
        for field, default in {
            "codice": "",
            "cap": "",
            "provincia": "",
            "regione": "",
            "lat": 0.0,
            "lon": 0.0
        }.items():
            if f"{field}_form" not in st.session_state:
                st.session_state[f"{field}_form"] = default
    
        # Aggiorna campi auto se selezionata una città
        if selected_comune:
            city_data = df_comuni[df_comuni['comune'] == selected_comune]
            if not city_data.empty:
                st.session_state.codice_form = city_data['codice'].iloc[0]
                st.session_state.provincia_form = city_data['provincia'].iloc[0]
                st.session_state.regione_form = city_data['regione'].iloc[0]
                st.session_state.lat_form = float(city_data['lat'].iloc[0])
                st.session_state.lon_form = float(city_data['lon'].iloc[0])
                # CAP viene aggiornato solo se vuoto o corrisponde al precedente auto
                if not st.session_state.cap_form or st.session_state.cap_form == st.session_state.get("last_auto_cap", ""):
                    st.session_state.cap_form = str(city_data['cap'].iloc[0])
                    st.session_state.last_auto_cap = st.session_state.cap_form
    
       # 📍 Titolo sezione auto-compilata
st.markdown("#### 📍 Dati Comune selezionato (auto)")

# ✅ Riquadro rosso per i dati auto-compilati
st.markdown("""
    <div style="
        border: 2px solid #cc0000;
        padding: 15px;
        border-radius: 10px;
        background-color: #fff5f5;
        margin-bottom: 20px;
    ">
""", unsafe_allow_html=True)

col1, col2 = st.columns(2)
with col1:
    st.markdown('<span style="color:#cc0000;">Codice Comune</span>', unsafe_allow_html=True)
    st.text_input("", value=st.session_state.codice_form, disabled=True, label_visibility="collapsed")

    st.markdown('<span style="color:#cc0000;">CAP (modificabile)</span>', unsafe_allow_html=True)
    st.text_input("", value=st.session_state.cap_form, key="cap_form", label_visibility="collapsed")

    st.markdown('<span style="color:#cc0000;">Provincia</span>', unsafe_allow_html=True)
    st.text_input("", value=st.session_state.provincia_form, disabled=True, label_visibility="collapsed")

    st.markdown('<span style="color:#cc0000;">Regione</span>', unsafe_allow_html=True)
    st.text_input("", value=st.session_state.regione_form, disabled=True, label_visibility="collapsed")

with col2:
    st.markdown('<span style="color:#cc0000;">Latitudine</span>', unsafe_allow_html=True)
    st.number_input("", value=st.session_state.lat_form, format="%.6f", key="lat_form", label_visibility="collapsed")

    st.markdown('<span style="color:#cc0000;">Longitudine</span>', unsafe_allow_html=True)
    st.number_input("", value=st.session_state.lon_form, format="%.6f", key="lon_form", label_visibility="collapsed")

    # st.markdown("</div>", unsafe_allow_html=True)  # chiusura riquadro rosso
    # 📍 Titolo sezione auto-compilata
    st.markdown("#### 📍 Dati Comune selezionato (auto)")
    
    # ✅ Riquadro rosso per i dati auto-compilati
    st.markdown("""
        <div style="
            border: 2px solid #cc0000;
            padding: 15px;
            border-radius: 10px;
            background-color: #fff5f5;
            margin-bottom: 20px;
        ">
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<span style="color:#cc0000;">Codice Comune</span>', unsafe_allow_html=True)
        st.text_input("", value=st.session_state.codice_form, disabled=True, label_visibility="collapsed")
    
        st.markdown('<span style="color:#cc0000;">CAP (modificabile)</span>', unsafe_allow_html=True)
        st.text_input("", value=st.session_state.cap_form, key="cap_form", label_visibility="collapsed")
    
        st.markdown('<span style="color:#cc0000;">Provincia</span>', unsafe_allow_html=True)
        st.text_input("", value=st.session_state.provincia_form, disabled=True, label_visibility="collapsed")
    
        st.markdown('<span style="color:#cc0000;">Regione</span>', unsafe_allow_html=True)
        st.text_input("", value=st.session_state.regione_form, disabled=True, label_visibility="collapsed")
    
    with col2:
        st.markdown('<span style="color:#cc0000;">Latitudine</span>', unsafe_allow_html=True)
        st.number_input("", value=st.session_state.lat_form, format="%.6f", key="lat_form", label_visibility="collapsed")
    
        st.markdown('<span style="color:#cc0000;">Longitudine</span>', unsafe_allow_html=True)
        st.number_input("", value=st.session_state.lon_form, format="%.6f", key="lon_form", label_visibility="collapsed")
    
    st.markdown("</div>", unsafe_allow_html=True)  # chiusura riquadro rosso
    
    # ➕ Titolo sezione form manuale
    st.markdown("#### ➕ Inserimento manuale punto vendita")
    
    # ✅ Riquadro per form manuale
    st.markdown("""
        <div style="
            border: 2px solid #e0e0e0;
            padding: 15px;
            border-radius: 10px;
            background-color: #fefefe;
            margin-bottom: 20px;
        ">
    """, unsafe_allow_html=True)
    
    with st.form("form_manuale"):
        col1, col2 = st.columns(2)
        with col1:
            selected_brand_form = st.selectbox("Seleziona Brand/Formato *", options=brand_list, key="brand_form")
            punto_vendita = st.text_input("Punto Vendita *", key="punto_vendita_form")
            indirizzo = st.text_input("Indirizzo *", key="indirizzo_form")
            note = st.text_area("Note", key="note_form")
        with col2:
            ultimo_intervento = st.date_input("Data Ultimo Intervento", key="ultimo_intervento_form")
            prossimo_intervento = st.date_input("Data Prossimo Intervento", key="prossimo_intervento_form")
            attrezzature = st.text_area("Attrezzature", key="attrezzature_form")
            referente_pv = st.text_input("Referente", key="referente_pv_form")
            telefono = st.text_input("Telefono", key="telefono_form")
    
            # 🔹 Spaziatura e bottoni centrati
        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.form_submit_button("🔄 Resetta dati Form", on_click=lambda: reset_form_fields())
            st.markdown("<div style='margin:20px 0;'></div>", unsafe_allow_html=True)
            submitted = st.form_submit_button("🔒 CONFERMA INSERIMENTO", type="primary")
    
        # ✅ Il blocco if submitted DEVE essere allo stesso livello del with st.form
        if submitted:
            if not selected_brand_form or not punto_vendita or not indirizzo or not selected_comune:
                st.error("Compila i campi obbligatori contrassegnati con *")
            else:
                conn = get_connection()
                cursor = conn.cursor()
                try:
                    cursor.execute(f'''
                        INSERT INTO manutenzioni ({", ".join(MANUTENZIONI_COLUMNS)})
                        VALUES ({", ".join(["?"]*len(MANUTENZIONI_COLUMNS))})
                    ''', (
                        punto_vendita, indirizzo, st.session_state.cap_form,
                        selected_comune, st.session_state.provincia_form,
                        st.session_state.regione_form, ultimo_intervento,
                        prossimo_intervento, attrezzature, note,
                        st.session_state.lat_form, st.session_state.lon_form,
                        st.session_state.codice_form, selected_brand_form,
                        referente_pv, telefono
                    ))
                    conn.commit()
                    st.success("✅ Nuova attività aggiunta con successo!")
                    st.toast("Attività inserita!", icon="✅")
                    st.session_state.reset_form_flag = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante l'inserimento: {e}")
                finally:
                    conn.close()
    
    st.markdown("</div>", unsafe_allow_html=True)  # chiusura riquadro grigio


# Funzione helper per resettare solo i campi della form
def reset_form_fields():
    form_keys = [
        "brand_form", "punto_vendita_form", "indirizzo_form",
        "ultimo_intervento_form", "prossimo_intervento_form",
        "attrezzature_form", "note_form", "referente_pv_form", "telefono_form"
    ]
    for key in form_keys:
        if key in st.session_state:
            if "form" in key and "text" in key:
                st.session_state[key] = ""
            else:
                st.session_state[key] = None



                        
## FUNZIONE PROGRAMMAZIONE PER PAGINA PROGRAMMAZIONE

def save_programmazione_to_db(df, total_distance):
    """Salva un nuovo ordine di lavoro con tutti i campi, gestendo correttamente date e orari."""
    try:
        work_order_id = str(uuid.uuid4())
        conn = get_connection() 
        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(work_order_number) FROM programmazione")
        result = cursor.fetchone()
        next_number = 1 if result[0] is None else result[0] + 1
        
        df_to_save = df.copy()

        for index, row in df_to_save.iterrows():
            # --- GESTIONE DELLA DATA ---
            date_value = row['data_programmata']
            date_to_save = date_value.date() if pd.notna(date_value) and isinstance(date_value, pd.Timestamp) else None
            
            # --- GESTIONE DELL'ORARIO (LA CORREZIONE FINALE) ---
            # 1. Normalizza il valore (restituisce un oggetto datetime.time)
            time_obj = normalize_time(row.get('orario_previsto'))
            # 2. Converti l'oggetto datetime.time in una stringa formattata per il DB
            time_to_save = time_obj.strftime('%H:%M:%S') if time_obj is not None else None

            cursor.execute("""
                INSERT INTO programmazione (work_order_id, work_order_number, punto_vendita, indirizzo, cap, citta, provincia, tecnico_assegnato, data_programmata, distanza_totale, referente_pv, telefono, attrezzature, note, orario_previsto)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                work_order_id,
                next_number,
                row['punto_vendita'],
                row['indirizzo'],
                row.get('cap', ''),
                row['citta'],
                row.get('provincia', ''),
                row['tecnico_assegnato'],
                date_to_save,
                total_distance,
                row.get('referente_pv'),
                row.get('telefono'),
                row.get('attrezzature'),
                row.get('note'),
                time_to_save
                
            ))
        conn.commit()
        conn.close()
         # --- GESTIONE DEL MESSAGIO DI SUCCESSO PERSISTENTE ---
        st.session_state['last_save_success'] = {
            'message': f"Ordine '{df_to_save.iloc[0]['punto_vendita']}' salvato con successo!",
            'timestamp': datetime.datetime.now()
        }
        st.success("Ordine di lavoro creato con successo!")
        st.rerun()
        
    except Exception as e:
        st.error(f"Errore durante il salvataggio nel database: {e}")
        return False
    
def calculate_total_route_distance(df):
    """Calcola la distanza totale del percorso sommando le distanze tra punti consecutivi."""
    if len(df) < 2:
        return 0.0
    total_distance = 0.0
    # Assumiamo che l'ordine nel DataFrame sia l'ordine del percorso
    for i in range(len(df) - 1):
        point1 = (df.iloc[i]['lat'], df.iloc[i]['lon'])
        point2 = (df.iloc[i+1]['lat'], df.iloc[i+1]['lon'])
        total_distance += geodesic(point1, point2).km
    return total_distance

# GENERAZIONE PDF PROGRAMMAZIONE  funzione globale 

def sanitize_text(text):
    """
    Rimuove caratteri non standard o problematici dal testo per evitare errori in FPDF.
    Mantiene caratteri ASCII, spazi e lettere accentuate comuni.
    """
    if pd.isna(text):
        return ""
    # Sostituisci caratteri problematici comuni
    text = str(text).replace('—', '-') # Sostituisce em-dash con dash normale
    # Rimuove qualsiasi carattere che non sia ASCII standard, spazi o lettere accentuate
    return re.sub(r'[^\x00-\x7FÀ-ÿ\s]', '', text)





def create_pdf(dataframe):
    """
    Crea un PDF con un layout a due righe usando due tabelle separate per ogni record.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=15*mm, leftMargin=15*mm, topMargin=15*mm, bottomMargin=15*mm)
    elements = []
    styles = getSampleStyleSheet()
    
    # --- 1. INTESTAZIONE DEL DOCUMENTO ---
    elements.append(Paragraph(" Programma di Manutenzione", styles['Title']))
    elements.append(Spacer(1, 6*mm))

    # --- 2. INTESTAZIONE DINAMICA ---
    valid_dates = dataframe['data_programmata'].dropna().dt.strftime('%d/%m/%Y')
    date_str = ", ".join(sorted(valid_dates.unique())) if not valid_dates.empty else "Nessuna data specificata"
    pvs_str = ", ".join(sorted(dataframe['punto_vendita'].unique()))
    elements.append(Paragraph(f"<b>Programma Manutenzione del:</b> {date_str}", styles['Normal']))
    elements.append(Paragraph(f"<b>Punti Vendita:</b> {pvs_str}", styles['Normal']))
    elements.append(Spacer(1, 6*mm))

    # --- 3. DATI E STILI ---
    headers_line1 = ['Punto Vendita', 'Data', 'Orario', 'Referente', 'Telefono']
    headers_line2 = ['Attrezzature', 'Indirizzo', 'Città', 'CAP', 'Provincia']
    
    # Larghezze per le due tabelle (devono essere coerenti)
    widths_line1 = [65*mm, 30*mm, 25*mm, 50*mm, 30*mm]
    widths_line2 = [35*mm, 75*mm, 50*mm, 20*mm, 20*mm]
    
    # Stile comune per entrambe le tabelle
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.green), ('TEXTCOLOR', (0, 0), (-1, 0), colors.lightgrey),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'), ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'), ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('VALIGN', (0, 1), (-1, -1), 'TOP'), ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ])

    # --- 4. CICLO DI STAMPA CON DUE TABELLE ---
    for index, row in dataframe.iterrows():
        # --- Prima Riga ---
        data_line1 = [headers_line1]
        data_line1.append([
            row.get('punto_vendita', ''),
            row['data_programmata'].strftime('%d/%m/%Y') if pd.notna(row['data_programmata']) else '',
            row['orario_previsto'].strftime('%H:%M') if pd.notna(row['orario_previsto']) else '',
            row.get('referente_pv', ''),
            row.get('telefono', '')
        ])
        table1 = Table(data_line1, colWidths=widths_line1)
        table1.setStyle(style)
        elements.append(table1)

        # --- Seconda Riga ---
        data_line2 = [headers_line2]
        data_line2.append([
            row.get('attrezzature', ''),
            row.get('indirizzo', ''),
            row.get('citta', ''),
            row.get('cap', ''),
            row.get('provincia', '')
        ])
        table2 = Table(data_line2, colWidths=widths_line2)
        table2.setStyle(style)
        elements.append(table2)
        
        # Spazio tra i record
        elements.append(Spacer(1, 3*mm))

    # --- 5. COSTRUZIONE DEL PDF ---
    doc.build(elements)
    
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes
 
 ## FUNZIONE DI COMPLETAMENTO DEL WORK ORDER CONFERMA ATTIVITA MANUTENZIONE SVOLTE
   

def complete_work_order(work_order_id):
    """
    Sposta un ordine di lavoro in 'storico_prog', aggiorna 'ultimo_intervento' in 'manutenzioni',
    e sincronizza 'note', 'referente_pv', 'telefono' in 'programmazione'.
    """
    try:
        st.info("Inizio completamento dell'ordine...")

        conn = get_connection()
        cursor = conn.cursor()

        # --- 1️⃣ Recupera i dati dell'ordine dalla tabella attiva ---
        cursor.execute("""
            SELECT punto_vendita, data_programmata, referente_pv, telefono, note 
            FROM programmazione 
            WHERE work_order_id = ?
        """, (work_order_id,))
        order_data = cursor.fetchone()

        if not order_data:
            st.warning(f"Nessun ordine trovato per ID {work_order_id}.")
            conn.close()
            return

        pv, data_prog, ref_pv, tel, note = order_data

        # --- 2️⃣ Aggiorna la tabella manutenzioni ---
        updates, params = [], []

        if pd.notna(data_prog) and isinstance(data_prog, (datetime.date, datetime.datetime)):
            updates.append("ultimo_intervento = ?")
            params.append(data_prog)

        if ref_pv:
            updates.append("referente_pv = ?")
            params.append(ref_pv)

        if tel:
            updates.append("telefono = ?")
            params.append(tel)

        if note:
            updates.append("note = ?")
            params.append(note)

        if updates:
            params.append(pv)
            query = f"UPDATE manutenzioni SET {', '.join(updates)} WHERE punto_vendita = ?"
            cursor.execute(query, tuple(params))
            st.success(f"Aggiornata tabella 'manutenzioni' per il punto vendita {pv}.")
        else:
            st.info("Nessun campo da aggiornare in manutenzioni.")

        # --- 3️⃣ Sposta l’ordine nello storico ---
        cursor.execute("SELECT COUNT(*) FROM programmazione WHERE work_order_id = ?", (work_order_id,))
        count_before_delete = cursor.fetchone()[0]
        st.write(f"DEBUG: trovati {count_before_delete} record in 'programmazione' da spostare.")

        cursor.execute("""
            INSERT INTO storico_prog
            SELECT * FROM programmazione WHERE work_order_id = ?
        """, (work_order_id,))
        st.write(f"DEBUG: Inserito {cursor.rowcount} record in 'storico_prog'.")

        cursor.execute("DELETE FROM programmazione WHERE work_order_id = ?", (work_order_id,))
        st.write(f"DEBUG: Cancellato {cursor.rowcount} record da 'programmazione'.")

        conn.commit()
        conn.close()

        st.success(f"✅ Ordine {work_order_id} completato e spostato nello storico!")
        st.balloons()
        st.rerun()

    except Exception as e:
        st.error(f"❌ Errore durante il completamento dell'ordine: {e}")
        try:
            conn.rollback()
        except:
            pass
        if conn:
            conn.close()

                
## PAGINA MAPPA INTERATTIVA
# ... (le tue import e le altre funzioni globali come create_pdf, init_db, etc.)

def show_mappa():
    st.header("🗺️ Mappa Interattiva e Pianificazione Interventi")
    
     # --- VISUALIZZAZIONE DEL MESSAGIO DI SUCCESSO ---
    if 'last_save_success' in st.session_state:
        st.success(st.session_state['last_save_success']['message'])
    

    if 'selected_for_work_order' not in st.session_state:
        st.session_state.selected_for_work_order = pd.DataFrame()
    if 'work_order_data' not in st.session_state:
        st.session_state.work_order_data = pd.DataFrame()

    try:
        # --- SEZIONE 1: MAPPA E FILTRI (codice precedente, dovrebbe funzionare) ---
        df_comuni = load_data("comuni")
        df_manutenzioni = load_data("manutenzioni")
        # ... (tutta la logica di caricamento e filtraggio che avevamo prima) ...
        if 'comune' not in df_comuni.columns:
           st.error("ERRORE CRITICO: La colonna 'comune' non è presente...")
           st.stop()
        df_map = df_manutenzioni.dropna(subset=['lat', 'lon']).copy()
        if df_map.empty: st.warning("Nessun punto vendita con coordinate disponibili..."); return
        df_map['ultimo_intervento_dt'] = pd.to_datetime(df_map['ultimo_intervento'], errors='coerce')
        def get_status(last_service_date_dt):
            if pd.isna(last_service_date_dt): return "Sconosciuto", "gray", "question"
            try:
                today = datetime.date.today()
                months_diff = (today.year - last_service_date_dt.year) * 12 + (today.month - last_service_date_dt.month)
                if months_diff > 12: return "Scaduto (>12 mesi)", "darkred", "exclamation-circle"
                elif 8 < months_diff <= 12: return "Attenzione (8-12 mesi)", "orange", "exclamation-circle"
                elif 4 < months_diff <= 8: return "In Scadenza (4-8 mesi)", "blue", "info-circle"
                else: return "OK (<4 mesi)", "green", "check-circle"
            except Exception: return "Errore Data", "black", "times-circle"
        df_map[['status', 'color', 'icon']] = df_map['ultimo_intervento_dt'].apply(lambda x: pd.Series(get_status(x)))
        st.sidebar.subheader("Filtri Mappa")
        filter_type = st.sidebar.radio("Tipo di Filtro", ["Nessuno", "Raggio (Km)", "N più Vicini"])
        if filter_type != "Nessuno":
            df_brands = load_data("format")
            brand_list = ["Tutti"] + df_brands['brand'].tolist()
            selected_brand_map = st.sidebar.selectbox("Filtra per Brand", brand_list)
            if selected_brand_map != "Tutti": df_map = df_map[df_map['brand'] == selected_brand_map]
        unique_cities = df_comuni['comune'].dropna().unique()
        city_list = [""] + sorted(unique_cities.tolist())
        selected_city = st.sidebar.selectbox("Centra Mappa su / Filtra per Città", city_list)
        center_lat, center_lon = 45.4367, 9.2072
        if selected_city:
            city_data = df_comuni[df_comuni['comune'] == selected_city]
            if not city_data.empty:
                center_lat = city_data['lat'].iloc[0]
                center_lon = city_data['lon'].iloc[0]
        if filter_type == "Raggio (Km)":
            radius_km = st.sidebar.number_input("Raggio in Km", min_value=1, value=10)
            if selected_city:
                center_point = (center_lat, center_lon)
                distances = [geodesic(center_point, (lat, lon)).km for lat, lon in zip(df_map['lat'], df_map['lon'])]
                df_map['distance'] = distances
                df_map = df_map[df_map['distance'] <= radius_km]
            else: st.sidebar.warning("Seleziona una città per usare il filtro per raggio.")
        elif filter_type == "N più Vicini":
            n_count = st.sidebar.number_input("Numero di punti più vicini", min_value=1, value=5)
            if selected_city:
                center_point = (center_lat, center_lon)
                distances = [geodesic(center_point, (lat, lon)).km for lat, lon in zip(df_map['lat'], df_map['lon'])]
                df_map['distance'] = distances
                df_map = df_map.sort_values(by='distance').head(n_count)
            else: st.sidebar.warning("Seleziona una città per usare il filtro per prossimità.")
        elif filter_type == "Nessuno":
            if selected_city: df_map = df_map[df_map['citta'] == selected_city]
        
        df_map = df_map.copy()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
        pin_colors_hex = {"darkred": "#8B0000 ", "orange": "#FF8C00", "blue": "#0066CC", "green": "#008000", "gray": "#808080", "black": "#333333"}
        for _, row in df_map.iterrows():
            # ... (logica di creazione del marker rimane identica) ...
            distance_text = ""
            if 'distance' in df_map.columns and not pd.isna(row.get('distance')):
                distance_text = f"<br><b>🚗 Distanza da {selected_city or 'centro mappa'}:</b> {row['distance']:.2f} Km"
            popup_text = f"</b>{distance_text}<br>{row['punto_vendita']}<br>Brand: {row['brand']}<br>Indirizzo: {row['indirizzo']}, {row['citta']} ({row['provincia']})<br>Ultimo Intervento: {row['ultimo_intervento']}</b><br>Stato: {row['status']}<br>👤 Referente: {row['referente_pv']}<br>📞 Telefono: {row['telefono']}<br>📝 Note: {row['note']}"
            pin_color_hex = pin_colors_hex.get(row['color'], "#808080")
            icon_html = f"""<div style="background-color: {pin_color_hex}; width: 36px; height: 36px; border-radius: 50% 50% 50% 0; transform: rotate(-45deg); display: flex; align-items: center; justify-content: center; border: 2px solid white; box-shadow: 0 2px 5px rgba(0,0,0,0.3);"><i class="fa fa-{row['icon']}" style="color: white; transform: rotate(45deg); font-size: 18px;"></i></div>"""
            custom_icon = folium.DivIcon(html=icon_html, icon_size=(36, 36), icon_anchor=(18, 36), popup_anchor=(0, -36))
            folium.Marker(location=[row['lat'], row['lon']], popup=folium.Popup(popup_text, max_width=300), tooltip=row['punto_vendita'], icon=custom_icon).add_to(m)
        st_data = st_folium(m, width=900, height=600, key="main_interactive_map")

        # --- SEZIONE 2: TABELLA PER LA SELEZIONE ---
        st.subheader("1. Seleziona i Punti Vendita e Aggiungi Dettagli")
        df_map_selection = df_map.copy()
        df_map_selection['Seleziona'] = False
        
        # --- PRIMO DATA_EDITOR 
        edited_df = st.data_editor(
            df_map_selection[['Seleziona', 'punto_vendita', 'brand', 'citta', 'attrezzature', 'note', 'status']],
            column_config={  # Questo è un dizionario, non un set
                "Seleziona": st.column_config.CheckboxColumn("Seleziona", help="Seleziona per aggiungere all'ordine di lavoro"),
                "punto_vendita": st.column_config.TextColumn("Punto Vendita"),
                "brand": st.column_config.TextColumn("Brand"),
                "citta": st.column_config.TextColumn("Città"),
                "attrezzature": st.column_config.TextColumn("Nr.Attrezzature"),
                "note": st.column_config.TextColumn("Note"),
                "status": st.column_config.TextColumn("Stato Manutenzione")
            },
            disabled=["punto_vendita", "brand", "citta", "status"],
            hide_index=True,
            key="selection_editor"
        )
        
        selected_indices = edited_df[edited_df['Seleziona'] == True].index
        selected_rows_full = df_map.loc[selected_indices]
        
        if not selected_rows_full.empty:
            st.session_state.selected_for_work_order = selected_rows_full
            work_order_df = st.session_state.selected_for_work_order[[
                'punto_vendita', 'indirizzo', 'citta', 'cap', 'provincia', 
                'referente_pv', 'telefono', 'attrezzature', 'note'
            ]].copy()
             # --- CORREZIONE CHIAVE: Usa i nomi delle colonne del DATABASE ---
            if 'tecnico_assegnato' not in work_order_df.columns: work_order_df['tecnico_assegnato'] = ''
            if 'data_programmata' not in work_order_df.columns: work_order_df['data_programmata'] = None
            if 'orario_previsto' not in work_order_df.columns: work_order_df['orario_previsto'] = None
            work_order_df['data_programmata'] = pd.to_datetime(work_order_df['data_programmata'], errors='coerce')

            # --- SECONDO DATA_EDITOR 
            edited_work_order = st.data_editor(
                work_order_df,
                column_config={
                    "punto_vendita": st.column_config.TextColumn("Punto Vendita", disabled=True),
                    "indirizzo": st.column_config.TextColumn("Indirizzo", disabled=True),
                    "citta": st.column_config.TextColumn("Città", disabled=True),
                    "cap": st.column_config.TextColumn("CAP", disabled=True),
                    "provincia": st.column_config.TextColumn("Provincia", disabled=True),
                    "referente_pv": st.column_config.TextColumn("Referente PV"),
                    "telefono": st.column_config.TextColumn("Telefono"),
                    "attrezzature": st.column_config.TextColumn("Nr.attrezzature"),
                    "note": st.column_config.TextColumn("Note"),
                    "tecnico_assegnato": st.column_config.TextColumn("Tecnico Assegnato"), # <--- CHIAVE CORRETTA
                    "data_programmata": st.column_config.DateColumn("Data Programmata"),   # <--- CHIAVE CORRETTA
                    "orario_previsto": st.column_config.TimeColumn("Orario Previsto")
                },
                num_rows="dynamic",
                key="work_order_editor"
            )
    
            st.session_state.work_order_data = edited_work_order
            # ... (resto del codice per il salvataggio e il PDF)
                        # --- SEZIONE 3: CALCOLO DISTANZA E SALVATAGGIO ---
                        # --- SEZIONE 3: CALCOLO DISTANZA E SALVATAGGIO ---
            st.subheader("2. Salva la Programmazione")
            
            total_distance = calculate_total_route_distance(st.session_state.selected_for_work_order)
            st.info(f"🚗 Distanza totale stimata del percorso: **{total_distance:.2f} Km**")

            if st.button("🔒 Conferma e Salva Programmazione", type="primary"):
                if not st.session_state.work_order_data.empty:
                    # --- LOGICA DI SALVATAGGIO CORRETTA E ROBUSTA ---
                    
                    # 1. Partiamo dal DataFrame completo con tutte le informazioni originali
                    final_df_to_save = st.session_state.selected_for_work_order.copy()
                    
                    # 2. Identifichiamo le colonne che l'utente poteva modificare nell'editor
                    editable_cols = ['tecnico_assegnato', 'data_programmata', 'orario_previsto', 'referente_pv', 'telefono','attrezzature', 'note']
                    
                    # 3. Creiamo un dizionario per aggiornare i dati in modo efficiente
                    # Usiamo 'punto_vendita' come chiave
                    updates = st.session_state.work_order_data.set_index('punto_vendita')[editable_cols].to_dict('index')
                    
                    # 4. Iteriamo sul DataFrame finale e aggiorniamo solo i valori modificati
                    for index, row in final_df_to_save.iterrows():
                        pv = row['punto_vendita']
                        if pv in updates:
                            for col in editable_cols:
                                # Aggiorna il valore solo se esiste negli aggiornamenti
                                if col in updates[pv]:
                                    final_df_to_save.at[index, col] = updates[pv][col]
                    
                    # 5. Chiamiamo la funzione di salvataggio con il DataFrame completo e corretto
                    if save_programmazione_to_db(final_df_to_save, total_distance):
                        st.success("✅ Programmazione salvata con successo!")
                        st.balloons()
                        # Pulisci lo stato per la prossima programmazione
                        st.session_state.selected_for_work_order = pd.DataFrame()
                        st.session_state.work_order_data = pd.DataFrame()
                        st.rerun()
                    else:
                        st.error("❌ Errore! La programmazione non è stata salvata. Controlla i messaggi di errore sopra.")
                else:
                    st.warning("Non ci sono dati da salvare.")
     # --- VISUALIZZAZIONE DEL MESSAGIO DI SUCCESSO ---
        if 'last_save_success' in st.session_state:
            st.success(st.session_state['last_save_success']['message'])                
    except Exception as e:
        st.error(f"Si è verificato un errore: {e}")
        st.write("Ricarica la pagina o riavvia l'app.")
        
## PAGINA PROGRAMMAZIONE
  
def show_programmazione():
    st.header("🔧 Programmazione manutenzioni")

  # --- Definisci i contenuti di ogni tab ---
    
    # Contenuto per la tab "Ordini Attivi"
    def show_tab_attivi():
        st.subheader("📋 Gestione Ordini di Lavoro Attivi")
        
        conn = get_connection()
        df_prog = pd.read_sql_query("SELECT * FROM programmazione ORDER BY work_order_number DESC", conn)
        conn.close()
        df_prog['created_at'] = pd.to_datetime(df_prog['created_at'])

        if df_prog.empty:
            st.warning("Nessuna programmazione attiva. Vai alla mappa per crearne una nuova.")
            return

        summary_df = df_prog[['work_order_id', 'work_order_number', 'created_at']].drop_duplicates()
        summary_df['label'] = summary_df.apply(lambda row: f"Work Order {row['work_order_number']} - {row['created_at'].date()}", axis=1)
        selected_label = st.selectbox("Seleziona un ordine da gestire:", summary_df['label'], key="active_wo_selector")
        selected_id = summary_df.loc[summary_df['label'] == selected_label, 'work_order_id'].iloc[0]

        df_filtered = df_prog[df_prog['work_order_id'] == selected_id].copy()
        
        punti_vendita_list = df_filtered['punto_vendita'].unique()
        punti_vendita_str = ', '.join(punti_vendita_list)
        new_title = f"Dettagli {selected_label}: {punti_vendita_str}"
        st.markdown(f"<h3 style='font-size: 20px; color: #555;'>{new_title}</h3>", unsafe_allow_html=True)
        
        df_filtered['data_programmata'] = pd.to_datetime(df_filtered['data_programmata'])
        df_filtered['orario_previsto'] = pd.to_datetime(df_filtered['orario_previsto'], format='%H:%M:%S', errors='coerce').dt.time
        df_filtered['Da Eliminare'] = False

        edited_df = st.data_editor(
            df_filtered,
            column_config={
                "id": st.column_config.NumberColumn("ID DB", disabled=True),
                "work_order_id": st.column_config.TextColumn("Work Order ID", disabled=True),
                "work_order_number": st.column_config.NumberColumn("N. Ordine", disabled=True),
                "created_at": st.column_config.DatetimeColumn("Creato il", disabled=True),
                "punto_vendita": st.column_config.TextColumn("Punto Vendita", disabled=True),
                "indirizzo": st.column_config.TextColumn("Indirizzo", disabled=True),
                "cap": st.column_config.TextColumn("CAP", disabled=True),
                "citta": st.column_config.TextColumn("Città", disabled=True),
                "provincia": st.column_config.TextColumn("Provincia", disabled=True),
                "tecnico_assegnato": st.column_config.TextColumn("Tecnico Assegnato"),
                "data_programmata": st.column_config.DateColumn("Data Programmata"),
                "referente_pv": st.column_config.TextColumn("Referente PV"),
                "telefono": st.column_config.TextColumn("Telefono"),
                "attrezzature": st.column_config.TextColumn("Attrezzature"),
                "note": st.column_config.TextColumn("Note"),
                "orario_previsto": st.column_config.TimeColumn("Orario Previsto"),
                "distanza_totale": st.column_config.NumberColumn("Distanza Totale (km)", disabled=True),
                "Da Eliminare": st.column_config.CheckboxColumn("Elimina Riga")
            },
            num_rows="dynamic",
            key="editor_programmazione"
        )

        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            #if st.button("💾 Salva Modifiche", type="primary", on_click=lambda: save_and_rerun, args=(edited_df,), help="Salva le modifiche e aggiorna la pagina."):
             #   save_and_rerun(edited_df)
            
            if st.button("💾 Salva Modifiche", type="primary"):
                save_changes(edited_df)
                
        with col2:
            if st.button("🗑️ Elimina Righe Selezionate"):
                delete_selected_rows(edited_df)
        with col3:
            st.button("⚠️ Elimina Intero Ordine", on_click=delete_entire_work_order, args=(selected_id,))
        
        st.markdown("---")
        st.subheader("Genera e Scarica PDF")
        
        if 'generated_pdf_id' in st.session_state and st.session_state['generated_pdf_id'] == selected_id:
            st.success("✅ Anteprima pronta")
            pdf_viewer(st.session_state['pdf_bytes'], height=700)
            st.download_button(label="💾 Scarica PDF", data=st.session_state['pdf_bytes'], file_name=f"ordine_lavoro_{selected_label.replace(' ', '_').replace('/', '-')}.pdf", mime="application/pdf", use_container_width=True)
            if st.button("🔄 Reset Anteprima"):
                del st.session_state['generated_pdf_id']
                del st.session_state['pdf_bytes']
                st.rerun()
        else:
            st.info("Nessuna anteprima generata.")
            if st.button("🔍 Genera Anteprima PDF"):
                pdf_df = edited_df.drop(columns=['id', 'work_order_id', 'work_order_number', 'created_at', 'Da Eliminare'])
                pdf_bytes = create_pdf(pdf_df)
                st.session_state['generated_pdf_id'] = selected_id
                st.session_state['pdf_bytes'] = pdf_bytes
                st.rerun()
                
        # --- MODIFICA QUESTO BOTTONE ---
        if st.button("✅ Conferma Esecuzione e Sposta in Storico", type="primary"):
            if complete_work_order(selected_id):
                st.success("Ordine completato con successo e spostato nello storico!")
    
                
            
                st.success("Ordine completato con successo e spostato nello storico!")
                st.balloons()
                st.rerun()
        
    # Contenuto per la tab "Storico Ordini Completati"
    
    def show_tab_storico():
        st.subheader("📜 Storico attività manutenzione")
    
        conn = get_connection()
        df_storico = pd.read_sql_query("SELECT * FROM storico_prog ORDER BY work_order_number DESC", conn)
        conn.close()
    
        df_storico['created_at'] = pd.to_datetime(df_storico['created_at'])
    
        if df_storico.empty:
            st.info("Nessun ordine completato nello storico.")
            return
            # --- Pulsante per svuotare completamente lo storico (solo admin) ---
        if st.session_state.get("role") == "admin":
            with st.expander("⚠️ Gestione completa dello storico", expanded=False):
                st.warning("⚠️ Questa operazione eliminerà **tutti** i record nello storico delle attività manutenzione. Irreversibile!")
                if st.button("🗑️ Svuota tutto lo storico"):
                    confirm = st.checkbox("✅ Confermo di voler cancellare tutti i record", key="confirm_clear_storico")
                    if confirm:
                        try: 
                            conn = get_connection()
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM storico_prog")
                            conn.commit()
                            conn.close()
                            st.success("✅ Tutto lo storico è stato eliminato!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore durante l'eliminazione dello storico: {e}")
                    

        # --- ADMIN: selezione e cancellazione multipla ---
        if st.session_state.get("role") == "admin":
            st.markdown("### ⚠️ Gestione record (solo admin)")
            df_to_show = df_storico.copy()
            df_to_show.insert(0, "Seleziona", False)

            edited_df = st.data_editor(
                df_to_show,
                column_config={
                    "Seleziona": st.column_config.CheckboxColumn("Seleziona"),
                    "id": st.column_config.NumberColumn("ID DB", disabled=True),
                    "work_order_number": st.column_config.NumberColumn("N. Ordine", disabled=True),
                    "punto_vendita": st.column_config.TextColumn("Punto Vendita", disabled=True),
                    "data_programmata": st.column_config.DateColumn("Data Programmata", disabled=True),
                    "referente_pv": st.column_config.TextColumn("Referente PV", disabled=True),
                    "telefono": st.column_config.TextColumn("Telefono", disabled=True),
                    "note": st.column_config.TextColumn("Note", disabled=True),
                },
                num_rows="dynamic",
                use_container_width=True
            )

            selected_rows = edited_df[edited_df["Seleziona"] == True]
            ids_to_delete = selected_rows['id'].tolist()

            if ids_to_delete:
                with st.expander("⚠️ Conferma cancellazione dei record selezionati", expanded=True):
                    st.warning(f"Stai per eliminare {len(ids_to_delete)} record. Questa operazione è irreversibile!")
                    if st.button("✅ Cancella i record selezionati"):
                        conn = get_connection()
                        cursor = conn.cursor()
                        try:
                            placeholders = ','.join(['?'] * len(ids_to_delete))
                            cursor.execute(f"DELETE FROM storico_prog WHERE id IN ({placeholders})", ids_to_delete)
                            conn.commit()
                            st.success(f"{len(ids_to_delete)} record eliminati con successo!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore durante la cancellazione: {e}")
                        finally:
                            conn.close()

        st.markdown("---")
    
        # --- Contenuto principale a tutta larghezza ---
        summary_storico_df = df_storico[['work_order_id', 'work_order_number', 'created_at']].drop_duplicates()
        summary_storico_df['label'] = summary_storico_df.apply(
            lambda row: f"Work Order {row['work_order_number']} - {row['created_at'].date()}", axis=1
        )
        selected_storico_label = st.selectbox(
            "Seleziona un ordine storico da visualizzare:", 
            summary_storico_df['label'], 
            key="storico_wo_selector"
        )
        selected_storico_id = summary_storico_df.loc[
            summary_storico_df['label'] == selected_storico_label, 'work_order_id'
        ].iloc[0]
    
        df_filtered_storico = df_storico[df_storico['work_order_id'] == selected_storico_id].copy()
        st.dataframe(df_filtered_storico.drop(columns=['work_order_id', 'id']), use_container_width=True)

        st.markdown("---")  # Linea di separazione

        # --- Pulsanti di download ---
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("📥 Storico Completo (Excel)"):
                excel_df = df_storico.drop(columns=['work_order_id', 'id'])
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    excel_df.to_excel(writer, index=False, sheet_name='Storico Ordini')
                st.download_button(
                    label="Download Excel",
                    data=output.getvalue(),
                    file_name=f"storico_ordini_completi_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        with col2:
            if st.button("🔍 Genera PDF Selezionato"):
                pdf_df_storico = df_filtered_storico.drop(columns=['work_order_id', 'id'])
                pdf_bytes = create_pdf(pdf_df_storico)
                st.download_button(
                    label="Download PDF",
                    data=pdf_bytes,
                    file_name=f"storico_ordine_{selected_storico_label.replace(' ', '_').replace('/', '-')}.pdf",
                    mime="application/pdf"
                )

        with col3:
            if st.button("📊 Tabella Visualizzata (Excel)"):
                excel_df_filtered = df_filtered_storico.drop(columns=['work_order_id', 'id'])
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    excel_df_filtered.to_excel(writer, index=False, sheet_name=f'Ordine {selected_storico_id[:8]}')
                st.download_button(
                    label="Download Excel",
                    data=output.getvalue(),
                    file_name=f"tabella_{selected_storico_id[:8]}_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        # --- Report analitico manutenzioni ---
        st.subheader("#📈 Report analitico manutenzioni")
        conn = get_connection()
        filtered = pd.DataFrame()

        try:
            df_report = pd.read_sql_query("SELECT * FROM report_attivita", conn)
            st.caption(f"Totale record nel report: {len(df_report)}")

            st.markdown("🔍 Filtra il report per punto vendita")
            pv_filter = st.text_input("Cerca punto vendita:")

            if pv_filter:
                filtered = df_report[df_report["punto_vendita"].str.contains(pv_filter, case=False, na=False)]
            else:
                filtered = df_report

            st.dataframe(filtered, use_container_width=True)

        except Exception as e:
            st.error(f"Errore nel caricamento della vista report_attivita: {e}")
        finally:
            conn.close()

    # --- Esportazione Excel report ---
        st.markdown(" 📤 Esporta il report filtrato in Excel")
        if not filtered.empty:
            try:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    filtered.to_excel(writer, index=False, sheet_name='ReportAttivita')

                st.download_button(
                    label="💾 Scarica Excel del report",
                    data=output.getvalue(),
                    file_name="report_attivita_manutenzione.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Errore durante la creazione del file Excel: {e}")
        else:
            st.warning("⚠️ Nessun dato disponibile da esportare. Verifica il filtro applicato.")

    # Contenuto per la tab "Stato Manutenzioni"
    def show_tab_stato():
        st.subheader("🔧 Stato Attuale Manutenzioni")
        
        conn = get_connection()
        df_manutenzioni = pd.read_sql_query("SELECT * FROM manutenzioni ORDER BY punto_vendita", conn)
        conn.close()
        
        if df_manutenzioni.empty:
            st.info("Nessun dato di manutenzione disponibile.")
        else:
            df_manutenzioni['ultimo_intervento'] = pd.to_datetime(df_manutenzioni['ultimo_intervento'])
            st.dataframe(df_manutenzioni, use_container_width=True)

    # --- Creazione delle tab ---
    tab1, tab2, tab3 = st.tabs(["📋 Ordini Attivi", "📜 Storico Completati", "🔧 Stato Manutenzioni"])
    
    with tab1:
        show_tab_attivi()
    with tab2:
        show_tab_storico()
    with tab3:
        show_tab_stato()
    # ... (all'interno della funzione show_programmazione)



# Le funzioni di supporto save_changes, delete_selected_rows, delete_entire_work_order rimangono invariate.

# Le funzioni di supporto save_changes, delete_selected_rows, delete_entire_work_order rimangono invariate.

# --- FUNZIONI DI SUPPORTO PER LA MODIFICA E CANCELLAZIONE ---

def save_changes(edited_df):
    """Salva le modifiche apportate nella tabella programmazione."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        for index, row in edited_df.iterrows():
            # --- GESTIONE DELLA DATA ---
            date_to_save = row['data_programmata'].date() if pd.notna(row['data_programmata']) and isinstance(row['data_programmata'], pd.Timestamp) else None
            
            # --- GESTIONE DELL'ORARIO (CONVERSIONE FINALE IN STRINGA) ---
            time_obj = normalize_time(row.get('orario_previsto'))
            time_to_save = time_obj.strftime('%H:%M:%S') if time_obj is not None else None
            
            # --- GESTIONE DELLA NOTA ---
            note_to_save = row.get('note', '')

            # --- AGGIORNA LA QUERY DI UPDATE ---
            cursor.execute("""
                UPDATE programmazione 
                SET tecnico_assegnato = ?, data_programmata = ?, referente_pv = ?, telefono = ?, orario_previsto = ?, note = ?
                WHERE id = ?
            """, (
                row['tecnico_assegnato'],
                date_to_save,
                row['referente_pv'],
                row['telefono'],
                time_to_save, # <--- Usa la stringa formattata
                note_to_save,
                row['id']
            ))
        conn.commit()
        conn.close()
        st.success("Modifiche salvate con successo!")
        st.rerun()
    except Exception as e:
        st.error(f"Errore durante il salvataggio delle modifiche: {e}")
        
def delete_selected_rows(edited_df):
    """Elimina le righe selezionate dalla tabella."""
    rows_to_delete = edited_df[edited_df['Da Eliminare'] == True]
    if not rows_to_delete.empty:
        try:
            conn = get_connection()
            cursor = conn.cursor()
            for record_id in rows_to_delete['id']:
                cursor.execute("DELETE FROM programmazione WHERE id = ?", (record_id,))
            conn.commit()
            conn.close()
            st.success(f"{len(rows_to_delete)} righe eliminate con successo!")
            st.rerun()
        except Exception as e:
            st.error(f"Errore durante l'eliminazione delle righe: {e}")
    else:
        st.warning("Nessuna riga selezionata per l'eliminazione.")       

def delete_entire_work_order(work_order_id):
    """Elimina l'intero ordine di lavoro dal database."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM programmazione WHERE work_order_id = ?", (work_order_id,))
        conn.commit()
        conn.close()
        st.success("Intero ordine di lavoro eliminato!")
        st.rerun()
    except Exception as e:
        st.error(f"Errore durante l'eliminazione dell'ordine di lavoro: {e}")


def normalize_time(value):
    """
    Converte qualsiasi tipo di dato relativo all'orario in un oggetto datetime.time standard.
    Gestisce anche formati con millisecondi.
    """
    if pd.isna(value):
        return None
    
    if isinstance(value, datetime.time):
        return value
    
    if isinstance(value, (datetime.datetime, pd.Timestamp)):
        return value.time()
        
    if isinstance(value, str):
        # --- AGGIUNGI I FORMATI CON MILLISECONDI ---
        formats_to_try = [
            '%H:%M:%S.%f',  # Es. '09:00:00.000'
            '%H:%M:%S',      # Es. '09:00:00'
            '%H:%M',         # Es. '09:00'
        ]
        for fmt in formats_to_try:
            try:
                return datetime.datetime.strptime(value, fmt).time()
            except (ValueError, TypeError):
                continue # Prova il formato successivo

    if isinstance(value, (int, float)):
        if 0 <= value < 1:
            total_seconds = value * 24 * 3600
            return (datetime.min + datetime.timedelta(seconds=total_seconds)).time()
    
    # Puoi rimuovere il debug ora che abbiamo trovato il problema
    # st.write(f"DEBUG: Orario non gestito: '{value}' di tipo {type(value)}")
    return None


def show_geocodifica():
    st.header("🌍 Geocodifica Indirizzi Mancanti")
    st.warning("Questa funzione trova le coordinate (latitudine, longitudine) per i record che non le hanno. Utilizza un servizio gratuito (Nominatim/OpenStreetMap) con un limite di richieste. Sii paziente.")
    
    df_manutenzioni = load_data("manutenzioni")
    df_to_geocode = df_manutenzioni[df_manutenzioni['lat'].isna() | df_manutenzioni['lon'].isna()].copy()
    
    if df_to_geocode.empty:
        st.success("Tutti i record hanno già le coordinate!")
        # Pulisci lo stato se non ci sono più record da geocodificare
        if 'geocode_editor_df' in st.session_state:
            del st.session_state.geocode_editor_df
        return

    st.write(f"Trovati {len(df_to_geocode)} record senza coordinate. Seleziona quelli da geocodificare.")
    
    # --- NUOVA LOGICA: USA SESSION STATE COME FONTE DI VERITÀ ---
    
    # 1. Inizializza o reinizializza il DataFrame in session_state
    # Lo reinizializziamo se il numero di record è cambiato (es. dopo una geocodifica parziale)
    needs_reinit = (
        'geocode_editor_df' not in st.session_state or 
        len(st.session_state.geocode_editor_df) != len(df_to_geocode)
    )
    if needs_reinit:
        df_to_geocode['Seleziona'] = False
        st.session_state.geocode_editor_df = df_to_geocode

    # 2. Il bottone "Seleziona Tutti" modifica il DataFrame in session_state
    if st.button("Seleziona tutti i record", key="select_all_geocode"):
        st.session_state.geocode_editor_df['Seleziona'] = True
        st.rerun() # Forza il riavvio per aggiornare la vista dell'editor

    # 3. L'editor usa il DataFrame dalla session_state come input
    edited_df = st.data_editor(
        st.session_state.geocode_editor_df, 
        use_container_width=True, 
        hide_index=True,
        key="geocode_editor" # Chiave univoca per questo widget
    )
    
    # 4. CRUCIALE: Sincronizza le modifiche manuali dell'utente con la session_state
    # Se l'utente deseleziona una riga dopo aver cliccato "Seleziona Tutti", questo aggiorna lo stato.
    st.session_state.geocode_editor_df = edited_df
    
    # 5. La logica di geocodifica ora usa il DataFrame dalla session_state
    selected_rows = st.session_state.geocode_editor_df[st.session_state.geocode_editor_df['Seleziona'] == True]
        
    if not selected_rows.empty and st.button("Avvia Geocodifica per i Selezionati", type="primary"):
        geolocator = Nominatim(user_agent="my_manutenzioni_app")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        conn = get_connection()
        cursor = conn.cursor()
        
        successful_updates = 0
        failed_updates = 0
        failed_records = []
        
        total_updates = len(selected_rows)
        current_progress = 0
        
        for index, row in selected_rows.iterrows():
            status_text.text(f"Geocodifica in corso per: {row['punto_vendita']}...")
            
            full_address = f"{row['indirizzo']}, {row['cap']}, {row['citta']}, {row['provincia']}, Italia"
            simple_address = f"{row['indirizzo']}, {row['citta']}, Italia"
            city_only_address = f"{row['citta']}, Italia"
            
            location = None
            address_used = "Nessuno"

            location = geolocator.geocode(full_address, timeout=10)
            if location:
                address_used = full_address

            if not location:
                st.warning(f"Indirizzo completo non trovato. Provo con '{simple_address}'...")
                location = geolocator.geocode(simple_address, timeout=10)
                if location:
                    address_used = simple_address

            if not location:
                st.warning(f"Indirizzo semplice non trovato. Provo solo con la città '{city_only_address}'...")
                location = geolocator.geocode(city_only_address, timeout=10)
                if location:
                    address_used = city_only_address
            
            if location:
                cursor.execute(
                    "UPDATE manutenzioni SET lat = ?, lon = ? WHERE ID = ?",
                    (location.latitude, location.longitude, row['ID'])
                )
                successful_updates += 1
                st.info(f"✅ Coordinate trovate per '{row['punto_vendita']}' usando: {address_used}")
            else:
                st.error(f"❌ Impossibile trovare le coordinate per '{row['punto_vendita']}'.")
                failed_updates += 1
                failed_records.append(row.to_dict())
            
            current_progress += 1
            progress_bar.progress(current_progress / total_updates)
            time.sleep(1)
        
        conn.commit()
        conn.close()
        
        status_text.text("Geocodifica completata!")
        st.success(f"Processo terminato. Aggiornati {successful_updates} record. Falliti {failed_updates} record.")
        
        if failed_records:
            st.session_state.failed_geocoding_df = pd.DataFrame(failed_records)
            st.error("Ci sono stati record falliti. Puoi modificarli e riprovare nella sezione sottostante.")
        else:
            if 'failed_geocoding_df' in st.session_state:
                del st.session_state.failed_geocoding_df

        # Pulisci lo stato per forzare una reinizializzazione al prossimo caricamento
        if 'geocode_editor_df' in st.session_state:
            del st.session_state.geocode_editor_df
        
        if successful_updates > 0:
            st.rerun()

            # --- SECONDA PARTE: MODIFICA E RIPROVA (CORRETTA) ---
    if 'failed_geocoding_df' in st.session_state and not st.session_state.failed_geocoding_df.empty:
        st.markdown("---")
        st.subheader("🔧 Modifica e Riprova Geocodifica")
        st.info("I record qui sotto non sono stati geocodificati. Puoi modificare l'indirizzo e poi riprovare per tutti i record falliti.")
        
        failed_edited_df = st.data_editor(
            st.session_state.failed_geocoding_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={"ID": st.column_config.NumberColumn("ID", disabled=True, width="small")},
            num_rows="dynamic"
        )

        if st.button("Riprova Geocodifica per i Record Falliti", type="secondary"):
            
            # Controllo di sicurezza per assicurarsi che sia un DataFrame
            if not isinstance(failed_edited_df, pd.DataFrame):
                st.error("Errore interno: i dati da riprovare non sono in un formato valido. Ricarica la pagina e riprova.")
                return

            if not failed_edited_df.empty:
                geolocator = Nominatim(user_agent="my_manutenzioni_app")
                retry_progress = st.progress(0)
                retry_status = st.empty()
                
                conn = get_connection()
                cursor = conn.cursor()
                
                retry_successful = 0
                retry_failed = 0
                
                # --- CORREZIONE CHIAVE: Usa un ciclo standard e più sicuro ---
                for index, row in failed_edited_df.iterrows():
                    retry_status.text(f"Riprovo per: {row['punto_vendita']}...")
                    
                    full_address = f"{row['indirizzo']}, {row['cap']}, {row['citta']}, {row['provincia']}, Italia"
                    location = geolocator.geocode(full_address, timeout=10)
                    
                    if location:
                        cursor.execute("UPDATE manutenzioni SET lat = ?, lon = ? WHERE ID = ?", (location.latitude, location.longitude, row['ID']))
                        retry_successful += 1
                        st.info(f"✅ Riuscito! Coordinate trovate per '{row['punto_vendita']}'.")
                        # Rimuovi il record dalla lista dei falliti in session_state
                        st.session_state.failed_geocoding_df = st.session_state.failed_geocoding_df[st.session_state.failed_geocoding_df['ID'] != row['ID']]
                    else:
                        retry_failed += 1
                        st.error(f"❌ Ancora fallito per '{row['punto_vendita']}'.")
                    
                    retry_progress.progress((index + 1) / len(failed_edited_df))
                    time.sleep(1)

                conn.commit()
                conn.close()
                
                retry_status.text("Retry completato!")
                st.success(f"Retry terminato. Riusciti: {retry_successful}, Falliti: {retry_failed}.")
                st.rerun()
            else:
                st.info("Nessun record fallito da riprovare.")

def show_import_export_dati():
    st.header("📤 Import / Export Dati")
    
    # Inizializza gli stati di sessione se non esistono
    if 'import_manutenzioni_success' not in st.session_state:
        st.session_state.import_manutenzioni_success = False
    if 'import_comuni_success' not in st.session_state:
        st.session_state.import_comuni_success = False

    # Creazione delle tab
    tab1, tab2, tab3 = st.tabs(["📂 Importa Manutenzioni", "🗂️ Importa Comuni (Setup)", "🌍 Geocodifica Indirizzi"])

    with tab1:
        st.subheader("Importa Dati Manutenzioni da Excel")
        st.write(f"Carica un file Excel (`.xlsx`). Dopo la validazione, dovrai confermare l'importazione. Il file deve avere le colonne: {', '.join(MANUTENZIONI_COLUMNS)}")
        uploaded_file = st.file_uploader("Scegli un file Excel delle manutenzioni", type=['xlsx'], key="manutenzioni_uploader")
        
        if uploaded_file is not None:
            try:
                new_data = pd.read_excel(uploaded_file)
                new_data.columns = new_data.columns.str.strip()

                st.info("1. Anteprima dati grezzi dal file Excel:")
                st.dataframe(new_data.head())
                st.write("Nomi delle colonne trovate:", list(new_data.columns))

                if set(new_data.columns) == set(MANUTENZIONI_COLUMNS):
                    st.success("Struttura colonne corretta. Puoi procedere con l'importazione.")
                    
                    # Prepara i dati (conversione date, etc.)
                    date_cols = ['ultimo_intervento', 'prossimo_intervento']
                    for col in date_cols:
                        if col in new_data.columns:
                            new_data[col] = pd.to_datetime(new_data[col], errors='coerce').dt.date
                    new_data = new_data.where(pd.notnull(new_data), None)

                    # Bottone di conferma per l'importazione
                    if st.button("Conferma e Aggiungi alla Tabella", type="primary"):
                        conn = get_connection()
                        try:
                            new_data.to_sql('manutenzioni', conn, if_exists='append', index=False)
                            st.success(f"✅ Importazione completata! {len(new_data)} righe sono state aggiunte al database.")
                            st.toast("Dati manutenzioni importati!", icon="📥")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore DB durante l'inserimento: {e}")
                        finally:
                            conn.close()
                else:
                    st.error("Errore: Le colonne del file Excel non corrispondono.")
                    st.write("**Colonne richieste:**", MANUTENZIONI_COLUMNS)
                    st.write("**Colonne trovate nel tuo file (dopo la pulizia):**", list(new_data.columns))
                    
            except Exception as e:
                st.error(f"Errore durante la lettura o il processamento del file: {e}")

    with tab2:
        st.subheader("Importa Elenco Comuni da Excel (Setup)")
        st.warning("Questa è un'operazione da eseguire una sola volta. Se eseguita di nuovo, sovrascriverà tutti i dati esistenti nella tabella 'comuni'.")
        st.write(f"Il file Excel deve avere esattamente queste colonne: {', '.join(COMUNI_COLUMNS)}")
        
        conn = get_connection()
        count = pd.read_sql_query("SELECT COUNT(*) as count FROM comuni", conn)['count'].iloc[0]
        conn.close()
        if count > 0:
            st.info(f"La tabella 'comuni' contiene già {count} record.")
        
        uploaded_file_comuni = st.file_uploader("Scegli il file Excel dei comuni", type=['xlsx'], key="comuni_uploader")
        
        if uploaded_file_comuni is not None:
            try:
                new_comuni = pd.read_excel(uploaded_file_comuni)
                new_comuni.columns = new_comuni.columns.str.strip()

                if set(new_comuni.columns) == set(COMUNI_COLUMNS):
                    st.success("Struttura colonne corretta. Puoi procedere con l'importazione.")
                    if st.button("Conferma e Importa Comuni", type="primary"):
                        conn = get_connection()
                        try:
                            new_comuni.to_sql('comuni', conn, if_exists='replace', index=False)
                            st.success(f"✅ Importazione completata! La tabella 'comuni' è stata popolata con {len(new_comuni)} comuni.")
                            st.toast("Dati comuni importati!", icon="🗂️")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Errore durante l'importazione nel DB: {e}")
                        finally:
                            conn.close()
                else:
                    st.error("Errore: Le colonne del file Excel non corrispondono.")
                    st.write("**Colonne richieste:**", COMUNI_COLUMNS)
                    st.write("**Colonne trovate:**", list(new_comuni.columns))
            except Exception as e:
                st.error(f"Errore durante la lettura del file Excel: {e}")

    with tab3:
        show_geocodifica()

def show_impostazioni():
    st.header("⚙️ Impostazioni")
    
    # --- LOGICA PER GESTIRE IL REPORT DIAGNOSTICO ---
    if 'diagnostic_report' in st.session_state:
        with st.expander("🔍 Report Diagnostico dell'Ultimo Reset", expanded=True):
            st.code(st.session_state.diagnostic_report, language='text')
            if st.button("Chiudi Report"):
                del st.session_state.diagnostic_report
                st.rerun()

    st.subheader("Gestione Brand/Formati Punto Vendita")
    st.write("Aggiungi qui i brand o i formati dei tuoi punti vendita (es. Carrefour Iper, Carrefour Market, etc.).")
    
    with st.form("add_brand_form"):
        new_brand = st.text_input("Nuovo Brand/Formato")
        submitted_brand = st.form_submit_button("Aggiungi Brand")
        if submitted_brand and new_brand:
            conn = get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("INSERT INTO format (brand) VALUES (?)", (new_brand.strip(),))
                conn.commit()
                st.success(f"Brand '{new_brand.strip()}' aggiunto con successo!")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error(f"Il brand '{new_brand.strip()}' esiste già.")
            except Exception as e:
                st.error(f"Errore: {e}")
            finally:
                conn.close()

    st.markdown("---")
    st.subheader("Brand/Formati Attualmente Disponibili:")
    df_brands_display = load_data("format")
    if not df_brands_display.empty:
        st.dataframe(df_brands_display, use_container_width=True, hide_index=True)
    else:
        st.info("Nessun brand aggiunto. Usare il form sopra per aggiungerne.")

    st.markdown("---")
    st.markdown("---")
    
    # --- NUOVA SEZIONE PER LA MIGRAZIONE ---
    st.subheader("🔧 Migrazione Tabella Manutenzioni (Correzione AUTOINCREMENT)")
    st.warning("Questa operazione modifica la struttura della tabella 'manutenzioni' per correggere il contatore ID. È sicura e va eseguita una sola volta.")
    st.write("Questa procedura:")
    st.write("1. Rinomina la tabella attuale in un backup temporaneo.")
    st.write("2. Crea una nuova tabella 'manutenzioni' con `ID INTEGER PRIMARY KEY AUTOINCREMENT`.")
    st.write("3. Copia tutti i dati dal backup alla nuova tabella.")
    st.write("4. Elimina il backup.")
    
    if st.button("Avvia Migrazione Tabella Manutenzioni", type="primary"):
        conn = get_connection()
        cursor = conn.cursor()
        try:
            st.info("Avvio migrazione...")
            # 1. Rinomina la tabella esistente
            cursor.execute("ALTER TABLE manutenzioni RENAME TO manutenzioni_old")
            
            # 2. Crea la nuova tabella con la struttura corretta
            cursor.execute('''
                CREATE TABLE manutenzioni (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    punto_vendita TEXT NOT NULL,
                    indirizzo TEXT NOT NULL,
                    cap TEXT,
                    citta TEXT NOT NULL,
                    provincia TEXT,
                    regione TEXT,
                    ultimo_intervento DATE,
                    prossimo_intervento DATE,
                    attrezzature TEXT,
                    note TEXT,
                    lat REAL,
                    lon REAL,
                    codice TEXT,
                    brand TEXT
                )
            ''')
            
            # 3. Copia i dati dalla vecchia tabella alla nuova
            cursor.execute("INSERT INTO manutenzioni (punto_vendita, indirizzo, cap, citta, provincia, regione, ultimo_intervento, prossimo_intervento, attrezzature, note, lat, lon, codice, brand) SELECT punto_vendita, indirizzo, cap, citta, provincia, regione, ultimo_intervento, prossimo_intervento, attrezzature, note, lat, lon, codice, brand FROM manutenzioni_old")
            
            # 4. Elimina la vecchia tabella
            cursor.execute("DROP TABLE manutenzioni_old")
            
            conn.commit()
            st.success("✅ Migrazione completata con successo! La tabella 'manutenzioni' ora ha il contatore ID corretto.")
            st.balloons()
            st.rerun()
        except Exception as e:
            st.error(f"Errore durante la migrazione: {e}")
            st.error("Se l'errore persiste, controlla il database. La tabella 'manutenzioni_old' potrebbe essere ancora presente.")
        finally:
            conn.close()

    st.markdown("---")
    st.markdown("---")

    st.subheader("🔴 Reset Tabella Manutenzioni")
    st.error("ATTENZIONE: Questa è un'operazione distruttiva e irreversibile. Cancellerà PER SEMPRE tutti i dati di manutenzione.")
    st.write("Per procedere, digita esattamente la parola `RESET` nel campo sottostante e conferma.")
    
    with st.form("reset_manutenzioni_form"):
        user_confirmation = st.text_input("Digita 'RESET' per confermare")
        submitted_reset = st.form_submit_button("Sì, AZZERA la tabella Manutenzioni", type="primary")
        
        if submitted_reset:
            if user_confirmation == "RESET":
                conn = get_connection()
                cursor = conn.cursor()
                report_lines = []
                try:
                    report_lines.append("🔍 Avvio della diagnostica...")
                    cursor.execute("SELECT COUNT(*) FROM manutenzioni")
                    count_before = cursor.fetchone()[0]
                    report_lines.append(f"- Record presenti nella tabella: {count_before}")
                    cursor.execute("SELECT MAX(ID) FROM manutenzioni")
                    max_id_before = cursor.fetchone()[0]
                    report_lines.append(f"- ID massimo attuale: {max_id_before if max_id_before else 'Nessuno'}")
                    cursor.execute("PRAGMA table_info(manutenzioni)")
                    table_info = cursor.fetchall()
                    is_autoincrement = any('AUTOINCREMENT' in str(info) for info in table_info)
                    report_lines.append(f"- La tabella ha AUTOINCREMENT: {'Sì' if is_autoincrement else 'No'}")
                    st.warning("🔧 Esecuzione dei comandi di reset in corso...")
                    cursor.execute("DELETE FROM manutenzioni")
                    report_lines.append("- Comando `DELETE FROM manutenzioni` eseguito.")
                    cursor.execute("DELETE FROM sqlite_sequence WHERE name='manutenzioni'")
                    report_lines.append("- Comando `DELETE FROM sqlite_sequence` eseguito.")
                    cursor.execute("SELECT * FROM sqlite_sequence WHERE name='manutenzioni'")
                    seq_row = cursor.fetchone()
                    report_lines.append(f"- Riga in 'sqlite_sequence' dopo la cancellazione: {seq_row}")
                    conn.commit()
                    st.session_state.diagnostic_report = "\n".join(report_lines)
                    st.success("✅ Comandi di reset eseguiti. Controlla il report diagnostico qui sopra.")
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    error_line = f"Errore durante il reset della tabella: {e}"
                    st.session_state.diagnostic_report = error_line
                    st.error(error_line)
                finally:
                    conn.close()
            else:
                st.error("Conferma non valida. Assicurati di aver digitato 'RESET' esattamente.")

# --- MAIN APPLICATION (CONTROLLER) ---

def main():
    # 🔁 Ripristina i database da GitHub solo se mancano in locale
    restore_from_github_simple()

    # 🧱 Inizializza i database (crea le tabelle se non esistono)
    init_db()
    init_login_log()

    # 🔐 Gestione login
    check_login()

    # 🧪 Opzionale: test connessione GitHub
    # test_github_connection()
    
    
    
    

    with st.sidebar:
        LOGO_PATH = "logo.png"
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, width=180)
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM programmazione")
            pending_count = cursor.fetchone()[0]
            conn.close()

            if pending_count > 0:
                if st.button(f"🔔 {pending_count} ordini di manutenzione pendenti -  per  completare  Vai alla Programmazione ", key="btn_pending_go"):
                    st.session_state["selected_page"] = "📊🔧 Programmazione"
                    st.rerun()
        except Exception as e:
                st.sidebar.warning(f"Impossibile caricare stato ordini: {e}") 
        st.markdown("---")
        st.markdown("🧭 **NAVIGAZIONE**")
        # --- 🔔 Avviso ordini pendenti ---
         
        
       

    # --- Definizione delle pagine ---
    page_options = {
        "🏛🎯 Gestione PV": show_gestione_manutenzioni,
        "📍🌍Mappa PV": show_mappa,
        "📊🔧 Programmazione": show_programmazione,
        "📤🗺️ Tools": show_import_export_dati,
        "⚙️ Impostazioni": show_impostazioni,
        
    }
    
     # 👑 Solo admin può vedere lo storico accessi
    if st.session_state.get("role") == "admin":
        page_options["📜 Storico Accessi Utenti"] = show_login_history
        
    pages_list = list(page_options.keys())
        
         
    
    # ✅ Inizializza correttamente lo stato
    if "selected_page" not in st.session_state:
        st.session_state["selected_page"] = pages_list[0]

    # --- Radio con chiave diversa per evitare conflitti ---
    selected_page = st.sidebar.radio(
        "Scegli un'azione",
        pages_list,
        index=pages_list.index(st.session_state["selected_page"]) if st.session_state["selected_page"] in pages_list else 0,
        key="page_radio"
    )

    # Se l’utente cambia pagina manualmente, aggiorna lo stato
    if selected_page != st.session_state["selected_page"]:
        st.session_state["selected_page"] = selected_page
        st.rerun()

    # --- Mostra la pagina selezionata ---
    page_func = page_options.get(st.session_state["selected_page"])
    if page_func:
        page_func()
    else:
        st.error(f"Pagina '{st.session_state['selected_page']}' non trovata.")
    
      # --- SEZIONE BACKUP E RIPRISTINO ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("🧑‍💻.github **Gestione Database**")

    if st.sidebar.button("💾 Salva database su GitHub"):
        backup_to_github_simple()
        
    last_backup = get_backup_timestamp()
    
    if last_backup:
        st.sidebar.caption(f"🕒 Ultimo backup: **{last_backup}**")
    else:
        st.sidebar.caption("🕒 Nessun backup registrato.")

    st.sidebar.caption("Backup automatico e ripristino da GitHub")

if __name__ == "__main__":
    main()





































































