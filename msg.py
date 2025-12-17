import argparse
import os
import time
import csv
import logging
import random
from datetime import datetime, timedelta
import re
import threading
import http.server
import socketserver
import json
from playwright.sync_api import sync_playwright

def parse_messages(names_arg):
    if names_arg.endswith('.txt'):
        if not os.path.exists(names_arg):
            print(f"File {names_arg} not found.")
            return []
        with open(names_arg, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    else:
        content = names_arg.strip()

    # Sirf & aur 'and' se hi split hoga
    content = content.replace(' and ', '&')
    messages = [msg.strip() for msg in content.split('&') if msg.strip()]
    return messages

def sender(tab_id, args, messages, headless, storage_path, stop_event, thread_url=None, row_data=None):
    global counters, counters_lock
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(storage_state=storage_path)
        page = context.new_page()
        dm_selector = 'div[role="textbox"][aria-label="Message"]'
        try:
            target = thread_url or args.thread_url
            page.goto(target, timeout=60000)
            page.wait_for_selector(dm_selector, timeout=30000)
            logging.info(f"Tab {tab_id} ready for {target}, starting message loop.")
            sent = 0
            backoff = float(getattr(args, 'backoff_base', 2.0))
            max_backoff = float(getattr(args, 'max_backoff', 300.0))
            delay = float(getattr(args, 'delay', 0.3))
            jitter = float(getattr(args, 'jitter', 0.0))
            max_count = int(getattr(args, 'count', 0))
            once = bool(getattr(args, 'once', False))
            while not stop_event.is_set():
                for msg in messages:
                    # Apply personalization if row_data is provided (safe formatting)
                    send_msg = msg
                    if row_data:
                        class _SafeDict(dict):
                            def __missing__(self, key):
                                return ""
                        try:
                            send_msg = msg.format_map(_SafeDict(row_data))
                        except Exception:
                            # fallback to original message on formatting error
                            send_msg = msg
                    if stop_event.is_set():
                        break
                    try:
                        # Rate limiting checks (shared counters)
                        if getattr(args, 'max_per_hour', 0) or getattr(args, 'max_per_day', 0):
                            from datetime import datetime as _dt
                            with counters_lock:
                                now = _dt.utcnow()
                                if now - counters['hour_start'] >= timedelta(hours=1):
                                    counters['hour_start'] = now.replace(minute=0, second=0, microsecond=0)
                                    counters['sent_hour'] = 0
                                if now - counters['day_start'] >= timedelta(days=1):
                                    counters['day_start'] = now.replace(hour=0, minute=0, second=0, microsecond=0)
                                    counters['sent_day'] = 0
                                if args.max_per_hour and counters['sent_hour'] >= args.max_per_hour:
                                    # sleep until next hour
                                    wait = (counters['hour_start'] + timedelta(hours=1) - now).total_seconds()
                                    logging.warning(f"Hourly limit reached ({args.max_per_hour}). Sleeping {int(wait)}s until next hour.")
                                    time.sleep(max(1, wait))
                                    continue
                                if args.max_per_day and counters['sent_day'] >= args.max_per_day:
                                    # sleep until next day
                                    wait = (counters['day_start'] + timedelta(days=1) - now).total_seconds()
                                    logging.warning(f"Daily limit reached ({args.max_per_day}). Sleeping {int(wait)}s until next day.")
                                    time.sleep(max(1, wait))
                                    continue

                        if not page.locator(dm_selector).is_visible():
                            logging.warning(f"Tab {tab_id} Selector not visible, skipping '{send_msg}'")
                            time.sleep(delay)
                            continue
                        page.click(dm_selector)
                        page.fill(dm_selector, send_msg)
                        page.press(dm_selector, 'Enter')
                        sent += 1
                        logging.info(f"Tab {tab_id} Sending ({sent}): {send_msg}")
                        # increment shared counters for rate limiting/monitoring
                        try:
                            with counters_lock:
                                counters['sent_hour'] += 1
                                counters['sent_day'] += 1
                        except Exception:
                            pass
                        backoff = float(getattr(args, 'backoff_base', 2.0))
                        # stop if requested to send each message only once or reached max count
                        if once:
                            logging.info(f"Tab {tab_id} Sent once mode active, stopping.")
                            stop_event.set()
                            break
                        if max_count > 0 and sent >= max_count:
                            logging.info(f"Tab {tab_id} Reached max count ({max_count}), stopping.")
                            stop_event.set()
                            break
                        # apply jitter to delay
                        actual_delay = delay
                        if jitter > 0:
                            actual_delay = max(0, delay + random.uniform(-jitter, jitter))
                        time.sleep(actual_delay)
                    except Exception as e:
                        logging.error(f"Tab {tab_id} Error sending message '{msg}': {e}")
                        # exponential backoff on errors
                        time.sleep(min(backoff, max_backoff))
                        backoff = min(backoff * 2, max_backoff)
        except Exception as e:
            logging.error(f"Tab {tab_id} Unexpected error: {e}")
        finally:
            browser.close()

def main():
    parser = argparse.ArgumentParser(description="Instagram DM Auto Sender using Playwright")
    parser.add_argument('--username', required=False, help='Instagram username')
    parser.add_argument('--password', required=False, help='Instagram password')
    parser.add_argument('--thread-url', required=True, help='Full Instagram direct thread URL')
    parser.add_argument('--names', required=True, help='Comma-separated, &-separated, or "and"-separated messages list (e.g., "Example 1& Example 2") or path to .txt file')
    parser.add_argument('--headless', default='true', help='true/false (optional, default true)')
    parser.add_argument('--storage-state', required=True, help='Path to JSON file to save/load login state')
    parser.add_argument('--tabs', type=int, default=1, help='Number of parallel tabs (1-3, default 1)')
    parser.add_argument('--count', type=int, default=0, help='Total messages per tab (0 = unlimited)')
    parser.add_argument('--delay', type=float, default=0.3, help='Delay between messages in seconds (default 0.3)')
    parser.add_argument('--once', action='store_true', help='Send each message once and stop')
    parser.add_argument('--recipients-file', required=False, help='Path to file containing thread URLs (one per line)')
    parser.add_argument('--max-per-hour', type=int, default=0, help='Maximum messages allowed per hour across all threads (0 = unlimited)')
    parser.add_argument('--max-per-day', type=int, default=0, help='Maximum messages allowed per day across all threads (0 = unlimited)')
    parser.add_argument('--jitter', type=float, default=0.0, help='Add random jitter (seconds) to delay')
    parser.add_argument('--backoff-base', type=float, default=2.0, help='Base seconds for exponential backoff on errors')
    parser.add_argument('--max-backoff', type=float, default=300.0, help='Maximum backoff seconds')
    parser.add_argument('--log-file', required=False, help='Path to a log file')
    parser.add_argument('--health-port', type=int, default=0, help='Optional local health HTTP port (0 = disabled)')

    args = parser.parse_args()

    headless = args.headless.lower() == 'true'
    storage_path = args.storage_state

    # Configure logging
    if args.log_file:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', filename=args.log_file, filemode='a')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    do_login = not os.path.exists(storage_path)
    if do_login:
        if not args.username or not args.password:
            print("Username and password required for login.")
            return
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context()
            page = context.new_page()
            try:
                print("Logging in...")
                page.goto("https://www.instagram.com/", timeout=60000)
                page.wait_for_selector('input[name="username"]', timeout=30000)
                page.fill('input[name="username"]', args.username)
                page.fill('input[name="password"]', args.password)
                page.click('button[type="submit"]')
                page.wait_for_url("https://www.instagram.com/", timeout=60000)
                print("Login successful, saving storage state.")
                context.storage_state(path=storage_path)
            except Exception as e:
                print(f"Login error: {e}")
            finally:
                browser.close()
    else:
        print("Loaded storage state, skipping login.")

    messages = parse_messages(args.names)
    if not messages:
        print("No messages provided.")
        return
    # Shared counters and window starts for rate limiting
    counters_lock = threading.Lock()
    counters = {
        'hour_start': datetime.utcnow().replace(minute=0, second=0, microsecond=0),
        'day_start': datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0),
        'sent_hour': 0,
        'sent_day': 0,
    }
    start_time = datetime.utcnow()
    stop_event = threading.Event()
    threads = []

    # If a recipients file is provided, read targets and spawn one sender per target.
    if args.recipients_file:
        if not os.path.exists(args.recipients_file):
            print(f"Recipients file {args.recipients_file} not found.")
            return
        # Detect CSV by extension
        _, ext = os.path.splitext(args.recipients_file)
        entries = []
        if ext.lower() == '.csv':
            with open(args.recipients_file, 'r', encoding='utf-8') as rf:
                reader = csv.DictReader(rf)
                headers = reader.fieldnames or []
                headers = [h.strip() for h in headers if h]
                if not headers:
                    print("Recipients CSV has no headers. Provide a header row (e.g. 'thread_url,name').")
                    return

                # determine url column
                url_candidates = ['thread_url', 'url', 'thread']
                url_field = None
                for c in url_candidates:
                    if c in headers:
                        url_field = c
                        break
                if not url_field:
                    # fallback to first column but warn
                    url_field = headers[0]
                    print(f"Warning: no standard URL column found in CSV. Using first column '{url_field}' as URL.")

                # detect placeholders used in messages
                placeholder_pattern = re.compile(r"{([A-Za-z0-9_]+)}")
                placeholders = set()
                for m in messages:
                    placeholders.update(placeholder_pattern.findall(m))

                missing_cols = []
                if placeholders:
                    for ph in placeholders:
                        if ph not in headers:
                            missing_cols.append(ph)
                    if missing_cols:
                        print("Recipients CSV is missing columns required for personalization:")
                        for mc in missing_cols:
                            print(f" - {mc}")
                        print("Either remove those placeholders from your messages or add matching CSV columns.")
                        return

                for row in reader:
                    url = (row.get(url_field) or '').strip()
                    if not url:
                        continue
                    entries.append((url, {k: (v or '').strip() for k, v in row.items()}))
        else:
            with open(args.recipients_file, 'r', encoding='utf-8') as rf:
                for line in rf:
                    line = line.strip()
                    if line:
                        entries.append((line, None))

        if not entries:
            print("No valid targets found in recipients file.")
            return

        print(f"Starting {len(entries)} recipient threads from {args.recipients_file}. Press Ctrl+C to stop.")
        for i, (target, row) in enumerate(entries, start=1):
            t = threading.Thread(target=sender, args=(i, args, messages, headless, storage_path, stop_event, target, row))
            t.daemon = False
            t.start()
            threads.append(t)
        # start health server if requested
        if args.health_port and args.health_port > 0:
            def run_health_server(port, stop_ev, counters_obj, counters_lck, thread_list, start_time_val):
                class Handler(http.server.BaseHTTPRequestHandler):
                    def do_GET(self_inner):
                        if self_inner.path in ('/', '/health'):
                            with counters_lck:
                                data = {
                                    'uptime_seconds': int((datetime.utcnow() - start_time_val).total_seconds()),
                                    'sent_hour': counters_obj.get('sent_hour', 0),
                                    'sent_day': counters_obj.get('sent_day', 0),
                                    'threads': len(thread_list),
                                }
                            self_inner.send_response(200)
                            self_inner.send_header('Content-Type', 'application/json')
                            self_inner.end_headers()
                            self_inner.wfile.write(json.dumps(data).encode())
                        else:
                            self_inner.send_response(404)
                            self_inner.end_headers()
                    def log_message(self_inner, format, *args):
                        return

                class TCPServer(socketserver.TCPServer):
                    allow_reuse_address = True

                server = TCPServer(('127.0.0.1', port), Handler)
                server.timeout = 1
                try:
                    while not stop_ev.is_set():
                        server.handle_request()
                finally:
                    server.server_close()

            hs = threading.Thread(target=run_health_server, args=(args.health_port, stop_event, counters, counters_lock, threads, start_time), daemon=True)
            hs.start()
    else:
        tabs = min(max(args.tabs, 1), 3)
        print(f"Starting {tabs} tabs. Press Ctrl+C to stop.")
        for i in range(tabs):
            t = threading.Thread(target=sender, args=(i+1, args, messages, headless, storage_path, stop_event))
            t.daemon = False
            t.start()
            threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping tabs...")
        stop_event.set()

    for t in threads:
        t.join(timeout=5)
    print("All tabs stopped.")

if __name__ == "__main__":
    main()