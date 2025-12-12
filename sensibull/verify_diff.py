
import sqlite3
import json
import requests
from app import normalize_trades_for_diff, calculate_diff

# Connect to DB to get a change ID
conn = sqlite3.connect('sensibull.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

# Get the latest change
change = c.execute("SELECT * FROM position_changes ORDER BY id DESC LIMIT 1").fetchone()
if not change:
    print("No changes found to verify.")
    exit()

change_id = change['id']
print(f"Verifying Change ID: {change_id}")

# Fetch data from our own API (to test the full flow)
response = requests.get(f'http://localhost:6060/api/diff/{change_id}')
if response.status_code != 200:
    print(f"API Error: {response.status_code} - {response.text}")
    exit()

data = response.json()
diff = data.get('diff', {})
current_positions = data.get('positions', [])
current_trades_normalized = normalize_trades_for_diff(current_positions)

# Now let's calculate what the PREVIOUS state must have been based on the diff
# Logic: Previous = Current - (Added - Removed - Modified_Diff)
# OR verifying: Previous + Diff = Current

# Since we don't have the previous state in the API response, we need to fetch it from DB to verify our math works
# But the user asked: "get the copy of diff and the previous data and then add them up to create the latest data"

# So let's fetch Previous snapshot from DB
snapshot_id = change['snapshot_id']
profile_id = change['profile_id']

prev_snapshot = c.execute("""
    SELECT * FROM snapshots 
    WHERE profile_id = ? AND id < ? 
    ORDER BY id DESC LIMIT 1
""", (profile_id, snapshot_id)).fetchone()

if not prev_snapshot:
    print("No previous snapshot found. Cannot verify diff math.")
    exit()

prev_raw = json.loads(prev_snapshot['raw_data'])
prev_trades_normalized = normalize_trades_for_diff(prev_raw.get('data', []))

print("\n--- verification Start ---")

# Reconstruct Current from Previous + Diff
reconstructed_current = {}

# 1. Start with Previous
import copy
reconstructed_current = copy.deepcopy(prev_trades_normalized)

# 2. Apply Diff
# Added: Add to reconstructed
for item in diff.get('added', []):
    key = f"{item['trading_symbol']}|{item['product']}"
    # We reconstruct the object as it would be in the map
    reconstructed_current[key] = {
        'trading_symbol': item['trading_symbol'],
        'product': item['product'],
        'quantity': item['quantity'],
        'average_price': item['average_price']
        # other fields might vary but these are the core identity + state
    }

# Removed: Remove from reconstructed
for item in diff.get('removed', []):
    key = f"{item['trading_symbol']}|{item['product']}"
    if key in reconstructed_current:
        del reconstructed_current[key]
    else:
        print(f"ERROR: Tying to remove {key} which is not in previous state!")

# Modified: Update reconstructed
for item in diff.get('modified', []):
    key = f"{item['trading_symbol']}|{item['product']}"
    if key in reconstructed_current:
        reconstructed_current[key]['quantity'] = item['quantity']
        # The API returns the *current* average price in the item object, so we update that too
        reconstructed_current[key]['average_price'] = item['average_price']
    else:
        print(f"ERROR: Trying to modify {key} which is not in previous state!")

# 3. Compare Reconstructed vs Actual Current
is_match = True
all_keys = set(reconstructed_current.keys()) | set(current_trades_normalized.keys())

for key in all_keys:
    rec = reconstructed_current.get(key)
    act = current_trades_normalized.get(key)
    
    if not rec:
        print(f"MISMATCH: Key {key} missing in Reconstructed, present in Actual.")
        is_match = False
    elif not act:
        print(f"MISMATCH: Key {key} present in Reconstructed, missing in Actual.")
        is_match = False
    else:
        # Compare core fields
        if rec['quantity'] != act['quantity']:
            print(f"MISMATCH {key}: Qty Rec={rec['quantity']} vs Act={act['quantity']}")
            is_match = False
        # Price might have float differences, allow small epsilon
        elif abs(rec['average_price'] - act['average_price']) > 0.01:
            print(f"MISMATCH {key}: Price Rec={rec['average_price']} vs Act={act['average_price']}")
            is_match = False

if is_match:
    print("\nSUCCESS: Reconstructed State (Prev + Diff) MATCHES Actual Current State.")
else:
    print("\nFAILURE: Verification failed.")

conn.close()
