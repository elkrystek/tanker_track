import json
import time
from datetime import datetime, timezone
from websocket import create_connection, WebSocketConnectionClosedException
import os
import requests

# Import the Google Cloud client libraries
from google.cloud import storage
from google.cloud import bigquery

# Zastąp 'config' bezpośrednim wczytywaniem zmiennej środowiskowej lub użyj Cloud Secret Manager
# from config import API_KEY 
# W przypadku Google Cloud zalecane jest użycie zmiennych środowiskowych lub Secret Manager
API_KEY = os.environ.get("AISSTREAM_API_KEY") 

# Inicjalizacja klientów Google Cloud
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Konfiguracja Google Cloud Storage i BigQuery
GCS_BUCKET_NAME = "your-ais-data-bucket" # Zmień na nazwę swojego wiadra GCS
BIGQUERY_PROJECT_ID = bigquery_client.project # Użyje domyślnego projektu, jeśli nie jest ustawiony w kliencie
BIGQUERY_DATASET_ID = "ais_data" # Nazwa zbioru danych BigQuery
BIGQUERY_POSITION_TABLE_ID = "position_reports" # Nazwa tabeli dla raportów pozycji
BIGQUERY_STATIC_TABLE_ID = "ship_static_data" # Nazwa tabeli dla danych statycznych

# Schematy BigQuery
# Możesz je zdefiniować tutaj lub pozwolić BigQuery na automatyczne wykrycie (zalecane dla początkujących)
# Pamiętaj, że automatyczne wykrycie może nie zawsze być idealne dla wszystkich typów danych.
POSITION_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("UserID", "INTEGER"),
    bigquery.SchemaField("Latitude", "FLOAT"),
    bigquery.SchemaField("Longitude", "FLOAT"),
    bigquery.SchemaField("SOG", "FLOAT"),
    bigquery.SchemaField("COG", "FLOAT"),
    bigquery.SchemaField("TrueHeading", "FLOAT"),
    bigquery.SchemaField("NavigationalStatus", "INTEGER"),
    bigquery.SchemaField("RateOfTurn", "FLOAT"),
    bigquery.SchemaField("ManeuverIndicator", "INTEGER"),
]

STATIC_SCHEMA = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("UserID", "INTEGER"),
    bigquery.SchemaField("ShipName", "STRING"),
    bigquery.SchemaField("CallSign", "STRING"),
    bigquery.SchemaField("IMO", "INTEGER"),
    bigquery.SchemaField("MMSI", "INTEGER"),
    bigquery.SchemaField("Type", "INTEGER"),
    bigquery.SchemaField("DimensionToBow", "INTEGER"),
    bigquery.SchemaField("DimensionToStern", "INTEGER"),
    bigquery.SchemaField("DimensionToPort", "INTEGER"),
    bigquery.SchemaField("DimensionToStarboard", "INTEGER"),
    bigquery.SchemaField("FixType", "INTEGER"),
    bigquery.SchemaField("ETA", "STRING"), # Zostawiamy jako STRING ze względu na format
    bigquery.SchemaField("MaximumStaticDraught", "FLOAT"),
    bigquery.SchemaField("Destination", "STRING"),
    bigquery.SchemaField("AisVersion", "INTEGER"),
]

def upload_to_gcs(data, filename):
    """Uploads a file-like object or string data to Google Cloud Storage."""
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(filename)
    blob.upload_from_string(data, content_type="application/json")
    print(f"☁️ Zapisano {filename} do GCS bucket: {GCS_BUCKET_NAME}")

def insert_into_bigquery(table_id, rows):
    """Inserts rows into a BigQuery table."""
    table_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID).table(table_id)
    errors = bigquery_client.insert_rows_json(table_ref, rows)

    if errors:
        print(f"❌ Błędy podczas wstawiania do BigQuery {table_id}: {errors}")
    else:
        print(f"✅ Pomyślnie wstawiono {len(rows)} wierszy do BigQuery {table_id}")

def _create_bigquery_dataset_and_tables():
    """Tworzy dataset i tabele BigQuery, jeśli nie istnieją."""
    dataset_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    try:
        bigquery_client.get_dataset(dataset_ref)
        print(f"Zbiór danych BigQuery '{BIGQUERY_DATASET_ID}' już istnieje.")
    except Exception:
        bigquery_client.create_dataset(dataset_ref)
        print(f"Utworzono zbiór danych BigQuery '{BIGQUERY_DATASET_ID}'.")

    # Tworzenie tabel
    tables_to_create = {
        BIGQUERY_POSITION_TABLE_ID: POSITION_SCHEMA,
        BIGQUERY_STATIC_TABLE_ID: STATIC_SCHEMA,
    }

    for table_name, schema in tables_to_create.items():
        table_ref = dataset_ref.table(table_name)
        try:
            bigquery_client.get_table(table_ref)
            print(f"Tabela BigQuery '{table_name}' już istnieje.")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            bigquery_client.create_table(table)
            print(f"Utworzono tabelę BigQuery '{table_name}'.")

def connect_ais_stream():
    """
    Establishes and maintains a WebSocket connection to the AISStream service,
    subscribes to specified AIS messages, and processes incoming data.
    Handles disconnections and attempts to reconnect.
    """
    # Upewnij się, że dataset i tabele BigQuery istnieją
    _create_bigquery_dataset_and_tables()

    while True:
        try:
            print("🔌 Nawiązywanie połączenia z AISStream...")
            ws = create_connection("wss://stream.aisstream.io:443/v0/stream", timeout=60)
            
            subscribe_message = {
                "APIKey": API_KEY,
                "FiltersShipMMSI": ["235108525", "311000516", "249955000"], # Usunięto duplikat
                "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
            }

            ws.send(json.dumps(subscribe_message))
            print("✅ Połączono i wysłano wiadomość subskrypcyjną.")

            while True:
                try:
                    message_json = ws.recv()
                    message = json.loads(message_json)
                    print("📨 Odebrano wiadomość:", json.dumps(message, indent=4))
                    
                    # Zapisz surową wiadomość do GCS
                    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
                    gcs_filename = f"raw_ais_messages/{timestamp_str}-{message.get('MessageType', 'unknown')}.json"
                    upload_to_gcs(message_json, gcs_filename)
                    
                    # send_to_render_api(message) # Jeśli nadal potrzebujesz tej funkcjonalności
                    
                    if message.get("MessageType") == "ShipStaticData":
                        save_ship_static_data_to_bigquery(message)
                    elif message.get("MessageType") == "PositionReport":
                        save_position_report_data_to_bigquery(message)

                except WebSocketConnectionClosedException:
                    print("⚠️ Połączenie zostało przerwane. Próba ponownego połączenia...")
                    break
                except Exception as e:
                    print(f"❌ Błąd podczas odbierania wiadomości: {e}")
                    break

        except KeyboardInterrupt:
            print("🛑 Zamykanie połączenia przez użytkownika...")
            break
        except Exception as e:
            print(f"❌ Błąd podczas łączenia: {e}")
            time.sleep(20)

        time.sleep(10)

def send_to_render_api(message):
    try:
        response = requests.post("https://twoja-nazwa.onrender.com/update", json=message)
        print(f"📤 Wysłano do Render: {response.status_code}")
    except Exception as e:
        print(f"❌ Błąd wysyłania do Render: {e}")

def save_ship_static_data_to_bigquery(message):
    """
    Saves ShipStaticData messages to a dedicated BigQuery table.
    """
    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "UserID": None, "ShipName": None, "CallSign": None, "IMO": None,
        "MMSI": None, "Type": None, "DimensionToBow": None,
        "DimensionToStern": None, "DimensionToPort": None,
        "DimensionToStarboard": None, "FixType": None, "ETA": None,
        "MaximumStaticDraught": None, "Destination": None, "AisVersion": None
    }

    if message["MessageType"] == "ShipStaticData":
        sd = message["Message"]["ShipStaticData"]
        row["UserID"] = sd.get("UserID")
        row["ShipName"] = sd.get("Name", "").strip()
        row["CallSign"] = sd.get("CallSign", "").strip()
        row["IMO"] = sd.get("IMO")
        row["MMSI"] = sd.get("MMSI")
        row["Type"] = sd.get("Type")

        dimensions = sd.get("Dimensions")
        if dimensions:
            row["DimensionToBow"] = dimensions.get("ToBow")
            row["DimensionToStern"] = dimensions.get("ToStern")
            row["DimensionToPort"] = dimensions.get("ToPort")
            row["DimensionToStarboard"] = dimensions.get("ToStarboard")

        row["FixType"] = sd.get("FixType")

        eta = sd.get("Eta")
        if eta:
            try:
                eta_str = f"{eta.get('Year', datetime.now().year)}-{eta.get('Month', 1):02d}-{eta.get('Day', 1):02d} {eta.get('Hour', 0):02d}:{eta.get('Minute', 0):02d}"
                row["ETA"] = eta_str
            except Exception:
                row["ETA"] = "Invalid ETA Format"

        draught = sd.get("MaximumStaticDraught")
        row["MaximumStaticDraught"] = float(draught) if draught is not None else None # Konwersja na float
        row["Destination"] = sd.get("Destination", "").strip()
        row["AisVersion"] = sd.get("AisVersion")
    
    insert_into_bigquery(BIGQUERY_STATIC_TABLE_ID, [row])

def save_position_report_data_to_bigquery(message):
    """
    Saves PositionReport messages to a dedicated BigQuery table.
    """
    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "UserID": None,
        "Latitude": None,
        "Longitude": None,
        "SOG": None,
        "COG": None,
        "TrueHeading": None,
        "NavigationalStatus": None,
        "RateOfTurn": None,
        "ManeuverIndicator": None
    }

    if message["MessageType"] == "PositionReport":
        pr = message["Message"]["PositionReport"]
        row["UserID"] = pr.get("UserID")
        row["Latitude"] = pr.get("Latitude")
        row["Longitude"] = pr.get("Longitude")
        row["SOG"] = pr.get("SOG")
        row["COG"] = pr.get("COG")
        row["TrueHeading"] = pr.get("TrueHeading")
        row["NavigationalStatus"] = pr.get("NavigationalStatus")
        row["RateOfTurn"] = pr.get("RateOfTurn")
        row["ManeuverIndicator"] = pr.get("ManeuverIndicator")
    
    insert_into_bigquery(BIGQUERY_POSITION_TABLE_ID, [row])

if __name__ == "__main__":
    # Ustaw zmienną środowiskową GOOGLE_APPLICATION_CREDENTIALS
    # Przed uruchomieniem skryptu lokalnie, musisz ustawić tę zmienną
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/service_account_key.json"
    connect_ais_stream()