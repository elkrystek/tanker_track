import json
import time
from datetime import datetime, timezone
from websocket import create_connection, WebSocketConnectionClosedException
import csv
import os


from config import API_KEY




def _write_to_csv(file_path, fieldnames, row):
    """
    Helper function to write a single row to a CSV file.
    It handles checking if the header needs to be written. This function always appends.
    """
    write_header = not os.path.exists(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    print(f"💾 Zapisano wiadomość do {file_path}")


def _update_or_add_csv_entry(file_path, fieldnames, new_row, key_field_name):
    """
    Helper function to update an existing row in a CSV file based on a key_field_name,
    or add a new row if the key is not found.
    """
    data = []
    found = False
    
    # Read existing data if file exists
    if os.path.exists(file_path):
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Ensure fieldnames are consistent, especially if the file was empty or had different headers
            if reader.fieldnames:
                # If existing fieldnames are different, we might need to handle this more robustly
                # For now, we assume they are consistent or new_row has all necessary fields
                pass 
            for row in reader:
                if row.get(key_field_name) == str(new_row.get(key_field_name)):
                    # Update existing row
                    data.append(new_row)
                    found = True
                else:
                    data.append(row)
    
    # If key not found, add new row
    if not found:
        data.append(new_row)

    # Write all data back to the CSV file
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader() # Always write header when overwriting the whole file
        writer.writerows(data)
    print(f"💾 Zaktualizowano/dodano wpis w {file_path} dla {key_field_name}: {new_row.get(key_field_name)}")


def connect_ais_stream():
    """
    Establishes and maintains a WebSocket connection to the AISStream service,
    subscribes to specified AIS messages, and processes incoming data.
    Handles disconnections and attempts to reconnect.
    """
    while True:
        try:
            print("🔌 Nawiązywanie połączenia z AISStream...")
            # Set a connection timeout to prevent indefinite blocking
            ws = create_connection("wss://stream.aisstream.io:443/v0/stream", timeout=60)
            # ws = create_connection("192.168.20.20", timeout=60)

            
            subscribe_message = {
                "APIKey": API_KEY,
                # Bounding box coordinates: [[top_latitude, left_longitude], [bottom_latitude, right_longitude]]
                # Helsinki (approx 60N, 25E) to Montevideo (approx 34S, 56W)
                # This covers a very large area, essentially most of the Atlantic.
                # "BoundingBoxes": [[[66, 69], [-34, -56]]],
                "BoundingBoxes": [[[62, -109], [-31, 80]]],
                # Filter for specific MMSI (Maritime Mobile Service Identity) numbers.
                # "FiltersShipMMSI": ["240657000", "248020000", "235118385", "538006312"],
                # Filter for specific AIS message types.
                "FilterMessageTypes": ["PositionReport", "ShipStaticData", "MultiSlotBinaryMessage"]
            }

            ws.send(json.dumps(subscribe_message))
            print("✅ Połączono i wysłano wiadomość subskrypcyjną.")

            while True:
                try:
                    # Receive messages from the WebSocket
                    message_json = ws.recv()
                    message = json.loads(message_json)
                    print("📨 Odebrano wiadomość:", json.dumps(message, indent=4))
                    
                    # Save the raw message to a JSON file and a combined CSV.
                    save_ais_message(message)
                    
                    # If the message is ShipStaticData, save it to its dedicated CSV file.
                    if message.get("MessageType") == "ShipStaticData":
                        save_ship_static_data(message)
                    # If the message is PositionReport, save it to its dedicated CSV file.
                    elif message.get("MessageType") == "PositionReport":
                        save_position_report_data(message)

                except WebSocketConnectionClosedException:
                    print("⚠️ Połączenie zostało przerwane. Próba ponownego połączenia...")
                    break  # Exit inner loop to trigger outer loop's reconnection attempt
                except Exception as e:
                    print(f"❌ Błąd podczas odbierania wiadomości: {e}")
                    break # Exit inner loop on other errors to attempt reconnection

        except KeyboardInterrupt:
            print("🛑 Zamykanie połączenia przez użytkownika...")
            break  # Exit outer loop on KeyboardInterrupt
        except Exception as e:
            print(f"❌ Błąd podczas łączenia: {e}")
            time.sleep(20)  # Wait before retrying connection on connection errors

        time.sleep(10)  # Short break before attempting to reconnect (if inner loop broke)


def save_ais_message(message):
    """
    Saves the incoming AIS message to a general JSON file and a combined CSV file.
    Extracts relevant fields for the CSV for PositionReport and ShipStaticData.
    This function handles a broad range of AIS messages.
    """
    # --- Save to JSON (list of messages) ---
    json_file = "ais_messages.json"
    if os.path.exists(json_file):
        with open(json_file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                # Handle case where file might be empty or corrupted
                data = []
    else:
        data = []

    data.append(message)
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"💾 Zapisano wiadomość do {json_file}")

    # --- Save to combined CSV (simplified) ---
    csv_file = "ais_messages.csv"
    # Define all possible fieldnames for the combined CSV
    fieldnames = [
        "timestamp", "MessageType", "UserID", "Latitude", "Longitude",
        "ShipName", "Destination", "MaximumStaticDraught", "ETA"
    ]

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(), # Use UTC for consistency
        "MessageType": message.get("MessageType"),
        "UserID": None,
        "Latitude": None,
        "Longitude": None,
        "ShipName": None,
        "Destination": None,
        "MaximumStaticDraught": None,
        "ETA": None
    }

    if message["MessageType"] == "PositionReport":
        pr = message["Message"]["PositionReport"]
        row["UserID"] = pr.get("UserID")
        row["Latitude"] = pr.get("Latitude")
        row["Longitude"] = pr.get("Longitude")
    elif message["MessageType"] == "ShipStaticData":
        sd = message["Message"]["ShipStaticData"]
        row["UserID"] = sd.get("UserID")
        row["ShipName"] = sd.get("Name", "").strip()
        row["Destination"] = sd.get("Destination", "").strip()
        # MaximumStaticDraught can be an integer or float, convert to string for CSV
        draught = sd.get("MaximumStaticDraught")
        row["MaximumStaticDraught"] = str(draught) if draught is not None else ""

        eta = sd.get("Eta")
        if eta:
            try:
                # Format ETA as YYYY-MM-DD HH:MM
                eta_str = f"{eta.get('Year', datetime.now().year)}-{eta.get('Month', 1):02d}-{eta.get('Day', 1):02d} {eta.get('Hour', 0):02d}:{eta.get('Minute', 0):02d}"
                row["ETA"] = eta_str
            except Exception:
                row["ETA"] = "Invalid ETA Format"

    _write_to_csv(csv_file, fieldnames, row)


def save_ship_static_data(message):
    """
    Saves ShipStaticData messages to a dedicated CSV file.
    This function is called only when the message type is "ShipStaticData".
    It updates an existing entry or adds a new one based on UserID.
    """
    csv_file = "ais_static_data.csv"
    fieldnames = [
        "timestamp", "UserID", "ShipName", "CallSign", "IMO", "MMSI",
        "Type", "DimensionToBow", "DimensionToStern", "DimensionToPort",
        "DimensionToStarboard", "FixType", "ETA", "MaximumStaticDraught",
        "Destination", "AisVersion"
    ]

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "UserID": None, "ShipName": None, "CallSign": None, "IMO": None,
        "MMSI": None, "Type": None, "DimensionToBow": None,
        "DimensionToStern": None, "DimensionToPort": None,
        "DimensionToStarboard": None, "Type": None, "ETA": None,
        "MaximumStaticDraught": None, "Destination": None, "AisVersion": None
    }

    if message["MessageType"] == "ShipStaticData":
        sd = message["Message"]["ShipStaticData"]
        row["UserID"] = sd.get("UserID")
        row["ShipName"] = sd.get("Name", "").strip()
        row["CallSign"] = sd.get("CallSign", "").strip()
        row["IMO"] = sd.get("IMO")
        row["MMSI"] = sd.get("MMSI") # MMSI is usually the same as UserID for ShipStaticData
        row["Type"] = sd.get("Type")

        # Dimensions
        dimensions = sd.get("Dimensions")
        if dimensions:
            row["DimensionToBow"] = dimensions.get("ToBow")
            row["DimensionToStern"] = dimensions.get("ToStern")
            row["DimensionToPort"] = dimensions.get("ToPort")
            row["DimensionToStarboard"] = dimensions.get("ToStarboard")

        row["FixType"] = sd.get("FixType")

        # ETA
        eta = sd.get("Eta")
        if eta:
            try:
                eta_str = f"{eta.get('Year', datetime.now().year)}-{eta.get('Month', 1):02d}-{eta.get('Day', 1):02d} {eta.get('Hour', 0):02d}:{eta.get('Minute', 0):02d}"
                row["ETA"] = eta_str
            except Exception:
                row["ETA"] = "Invalid ETA Format"

        # MaximumStaticDraught
        draught = sd.get("MaximumStaticDraught")
        row["MaximumStaticDraught"] = str(draught) if draught is not None else ""

        row["Destination"] = sd.get("Destination", "").strip()
        row["AisVersion"] = sd.get("AisVersion")

    _update_or_add_csv_entry(csv_file, fieldnames, row, "UserID")


def save_position_report_data(message):
    """
    Saves PositionReport messages to a dedicated CSV file.
    This function is called only when the message type is "PositionReport".
    It updates an existing entry or adds a new one based on UserID.
    """
    csv_file = "ais_position_reports.csv" # New file for position reports
    fieldnames = [
        "timestamp", "UserID", "Latitude", "Longitude", "SOG", "COG",
        "TrueHeading", "NavigationalStatus", "RateOfTurn", "ManeuverIndicator"
    ]

    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "UserID": None,
        "Latitude": None,
        "Longitude": None,
        "SOG": None, # Speed Over Ground
        "COG": None, # Course Over Ground
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
    
    _update_or_add_csv_entry(csv_file, fieldnames, row, "UserID")


if __name__ == "__main__":
    connect_ais_stream()
