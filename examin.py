import pandas as pd
import os

# === CONFIGURATION ===
input_folder = "/Users/arunramji/Desktop/Carelton2425"
output_log = "/Users/arunramji/Desktop/Carelton2425_error_log.txt"
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

# === LOGGING SETUP ===
log_entries = []

def log(message):
    print(message)
    log_entries.append(message)

# === PROCESS EACH FILE ===
for i, file in enumerate(csv_files, start=1):
    file_path = os.path.join(input_folder, file)
    try:
        log(f"--- Processing File {i}/{len(csv_files)}: {file} ---")
        data = pd.read_csv(file_path)

        # Check: column presence
        required_cols = ["team", "action", "player", "start", "end"]
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Check: data types
        for col in ["start", "end"]:
            if not pd.api.types.is_numeric_dtype(data[col]):
                raise TypeError(f"Column '{col}' is not numeric. Dtype: {data[col].dtype}")

        # Check: nulls
        nan_counts = data[required_cols].isnull().sum()
        if nan_counts.any():
            raise ValueError(f"Nulls found in required columns: {nan_counts[nan_counts > 0].to_dict()}")

        # Check: negative time intervals
        bad_intervals = data[data["end"] < data["start"]]
        if not bad_intervals.empty:
            raise ValueError(f"Found {len(bad_intervals)} rows where 'end' < 'start'")

        # File passed
        log(f"✅ File {file} passed all checks.\n")

    except Exception as e:
        log(f"❌ Error in file {file}: {e}\n")

# === SAVE LOG ===
with open(output_log, "w") as f:
    for entry in log_entries:
        f.write(entry + "\n")

log(f"\n🔍 Log written to {output_log}")


