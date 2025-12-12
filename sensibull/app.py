from flask import Flask, render_template, request, jsonify
import sqlite3
import json
from datetime import datetime
from database import get_db, sync_profiles

app = Flask(__name__)
# Configure standard port or 5010 as per previous context
PORT = 6060

@app.template_filter('to_datetime')
def to_datetime_filter(value):
    if isinstance(value, datetime):
        return value
    # Handle SQLite default string format: "YYYY-MM-DD HH:MM:SS"
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
             # Handle ISO format if present
             return datetime.fromisoformat(value)
        except:
             return value


@app.route('/')
def index():
    # Sync profiles from file on every refresh
    sync_profiles()
    
    conn = get_db()
    c = conn.cursor()
    
    # Get all profiles
    profiles_db = c.execute("SELECT * FROM profiles").fetchall()
    
    # Get order from urls.txt
    ordered_slugs = []
    try:
        urls_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'urls.txt')
        if os.path.exists(urls_path):
            with open(urls_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        slug = line.split('/')[-1] if 'sensibull.com' in line else line
                        ordered_slugs.append(slug)
    except Exception as e:
        print(f"Error reading urls.txt for sorting: {e}")

    # Create a map for sorting
    # profile_key -> index
    # We use a large number if not found so they go to the bottom
    sort_map = {slug: i for i, slug in enumerate(ordered_slugs)}
    
    # Sort the DB profiles: those in urls.txt first (in order), others after
    profiles = sorted(profiles_db, key=lambda p: sort_map.get(p['slug'], 99999))

    # Calculate dates (last 7 days?)
    # ... existing logic ...
    
    # Need to adapt existing logic because it probably did a single fetch
    # Let's inspect the original code in the next few lines in view_file usage
    # but I already see it.
    
    # We have profiles now. Now get dates.
    # Get unique dates from changes
    dates_rows = c.execute("SELECT DISTINCT date(timestamp) as day FROM position_changes ORDER BY day DESC LIMIT 7").fetchall()
    dates = [row['day'] for row in dates_rows]
    
    # Build matrix
    matrix = {} 
    for p in profiles:
        for d in dates:
            # Check if any changes on this day
            count = c.execute("""
                SELECT COUNT(*) FROM position_changes 
                WHERE profile_id = ? AND date(timestamp) = ?
            """, (p['id'], d)).fetchone()[0]
            matrix[(p['id'], d)] = count
            
    conn.close()
    return render_template('index.html', profiles=profiles, dates=dates, matrix=matrix)

@app.route('/profile/<slug>/<date>')
def daily_view(slug, date):
    conn = get_db()
    c = conn.cursor()
    
    profile = c.execute("SELECT * FROM profiles WHERE slug = ?", (slug,)).fetchone()
    if not profile:
        return "Profile not found", 404
    
    # Get changes for this date
    # SQLite 'date(timestamp)' matches 'YYYY-MM-DD'
    changes = c.execute("""
        SELECT * FROM position_changes 
        WHERE profile_id = ? AND date(timestamp) = ? 
        ORDER BY timestamp DESC
    """, (profile['id'], date)).fetchall()
    
    conn.close()
    return render_template('daily_view.html', slug=slug, date=date, changes=changes)

@app.route('/api/diff/<int:change_id>')
def api_diff(change_id):
    conn = get_db()
    c = conn.cursor()
    
    change = c.execute("SELECT * FROM position_changes WHERE id = ?", (change_id,)).fetchone()
    if not change:
        conn.close()
        return jsonify({'error': 'Change not found'}), 404
        
    current_snapshot = c.execute("SELECT * FROM snapshots WHERE id = ?", (change['snapshot_id'],)).fetchone()
    current_raw = json.loads(current_snapshot['raw_data']) if current_snapshot else {}
    current_trades = normalize_trades_for_diff(current_raw.get('data', []))

    # Find PREVIOUS snapshot for this profile
    # We want the latest snapshot BEFORE this one
    prev_snapshot = c.execute("""
        SELECT * FROM snapshots 
        WHERE profile_id = ? AND id < ? 
        ORDER BY id DESC LIMIT 1
    """, (change['profile_id'], change['snapshot_id'])).fetchone()
    
    prev_raw = json.loads(prev_snapshot['raw_data']) if prev_snapshot else {}
    prev_trades = normalize_trades_for_diff(prev_raw.get('data', []))
    
    # Calculate Diff
    diff_data = calculate_diff(prev_trades, current_trades)
    
    conn.close()
    return jsonify({
        'diff_summary': change['diff_summary'],
        'positions': current_raw.get('data', []), # Send full current positions for the bottom table
        'diff': diff_data
    })

@app.route('/api/daily_log/<slug>/<date>')
def daily_log(slug, date):
    conn = get_db()
    c = conn.cursor()
    
    profile = c.execute("SELECT * FROM profiles WHERE slug = ?", (slug,)).fetchone()
    if not profile:
        conn.close()
        return jsonify({'error': 'Profile not found'}), 404
        
    # fetch all changes for the day in chronological order
    changes = c.execute("""
        SELECT * FROM position_changes 
        WHERE profile_id = ? AND date(timestamp) = ? 
        ORDER BY timestamp ASC
    """, (profile['id'], date)).fetchall()
    
    events = []
    
    # We need to calculate diffs sequentially
    # Optimization: fetch all snapshots involved in one query if possible, or just iterate
    # Given expected volume is low, iterating is fine.
    
    processed_first = False
    
    for change in changes:
        curr_snap = c.execute("SELECT * FROM snapshots WHERE id = ?", (change['snapshot_id'],)).fetchone()
        curr_raw = json.loads(curr_snap['raw_data']) if curr_snap else {}
        curr_trades = normalize_trades_for_diff(curr_raw.get('data', []))
        
        # Determine previous state
        # If it's the very first snapshot ever, prev is empty.
        # Use database logic to find previous snapshot relative to THIS change's snapshot
        prev_snap = c.execute("""
            SELECT * FROM snapshots 
            WHERE profile_id = ? AND id < ? 
            ORDER BY id DESC LIMIT 1
        """, (profile['id'], change['snapshot_id'])).fetchone()
        
        prev_raw = json.loads(prev_snap['raw_data']) if prev_snap else {}
        prev_trades = normalize_trades_for_diff(prev_raw.get('data', []))
        
        diff = calculate_diff(prev_trades, curr_trades)
        
        # Flatten diff into events
        timestamp_str = change['timestamp'] # SQLite returns string usually
        try:
            # Try to format it nicely if it's a standard format
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            time_str = dt.strftime('%H:%M')
        except:
             # Try isoformat
            try:
                dt = datetime.fromisoformat(timestamp_str)
                time_str = dt.strftime('%H:%M')
            except:
                time_str = timestamp_str

        # Helper to add events
        def add_events(items, type_label):
            for item in items:
                qty_change = f"{item['quantity']}" 
                if type_label == 'MODIFIED':
                     sign = '+' if item['quantity_diff'] > 0 else ''
                     qty_change = f"{item['old_quantity']} -> {item['quantity']} ({sign}{item['quantity_diff']})"
                
                events.append({
                    'timestamp': time_str,
                    'full_timestamp': timestamp_str, # For sorting if needed
                    'type': type_label,
                    'symbol': item['trading_symbol'],
                    'product': item['product'],
                    'qty_change': qty_change,
                    'price': round(float(item['average_price']), 1)
                })

        add_events(diff['added'], 'ADDED')
        add_events(diff['removed'], 'REMOVED')
        add_events(diff['modified'], 'MODIFIED')
        
    conn.close()
    
    # Return events reversed (latest first) as requested "latest changes on the top row"
    events.reverse()
    
    return jsonify({'events': events})

def normalize_trades_for_diff(positions_data):
    """
    Extracts all trades and creates a signature map for easy comparison.
    Key: symbol|product|strike|option_type
    Value: Trade object (summed quantity if multiple trades exist for same key, though rare)
    """
    trades_map = {}
    for p in positions_data:
        for t in p.get('trades', []):
            # Create a unique key for the instrument
            key = f"{t.get('trading_symbol')}|{t.get('product')}"
            
            if key not in trades_map:
                trades_map[key] = {
                    'trading_symbol': t.get('trading_symbol'),
                    'product': t.get('product'),
                    'quantity': 0,
                    'average_price': 0,
                    'last_price': t.get('last_price'), # Keep for reference
                    'pnl': t.get('unbooked_pnl') # Keep for reference
                }
            
            # Weighted average for price if needed, but usually it's unique enough. 
            # Let's just sum quantity for now.
            current_qty = trades_map[key]['quantity']
            new_qty = int(t.get('quantity', 0))
            
            # Simple avg price update (approximate if multiple trades)
            total_val = (trades_map[key]['average_price'] * current_qty) + (float(t.get('average_price', 0)) * new_qty)
            trades_map[key]['quantity'] += new_qty
            if trades_map[key]['quantity'] != 0:
                trades_map[key]['average_price'] = total_val / trades_map[key]['quantity']
            
    return trades_map

def calculate_diff(prev_map, curr_map):
    added = []
    removed = []
    modified = []
    
    all_keys = set(prev_map.keys()) | set(curr_map.keys())
    
    for key in all_keys:
        p = prev_map.get(key)
        c = curr_map.get(key)
        
        if not p:
            # Added
            c['change_type'] = 'ADDED'
            added.append(c)
        elif not c:
            # Removed
            p['change_type'] = 'REMOVED'
            removed.append(p)
        else:
            # Check for modification (quantity change)
            if p['quantity'] != c['quantity']:
                c['change_type'] = 'MODIFIED'
                c['old_quantity'] = p['quantity']
                c['quantity_diff'] = c['quantity'] - p['quantity']
                modified.append(c)
                
    return {
        'added': added,
        'removed': removed,
        'modified': modified
    }

import sys
import os
import threading
import time

@app.route('/restart', methods=['POST'])
def restart_app():
    def restart():
        time.sleep(1) # Give time for response to be sent
        print("Restarting application...")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    
    threading.Thread(target=restart).start()
    return "Restarting application... Please reload the page in a few seconds.", 200

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=PORT)
