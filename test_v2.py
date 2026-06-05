import os
import traceback
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import TradeParams

# Load the root .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Apply proxy
PROXY_URL = os.getenv("PROXY_URL", "")
if PROXY_URL:
    import httpx
    # Need to patch http_helpers
    from py_clob_client.http_helpers import helpers as _clob_http
    _clob_http._http_client = httpx.Client(http2=True, proxy=PROXY_URL)

def run_tests():
    host = os.getenv("HOST", "https://clob.polymarket.com")
    key = os.getenv("PRIVATE_KEY")
    funder = os.getenv("FUNDER_ADDRESS")
    chain_id = int(os.getenv("CHAIN_ID", "137"))
    
    print("====================================")
    print(f"Testing Polymarket V2 Connection:")
    print(f"Host: {host}")
    print(f"Chain ID: {chain_id}")
    print(f"Funder: {funder}")
    print("====================================\n")
    
    try:
        # Initialize client
        client = ClobClient(
            host=host, 
            key=key, 
            chain_id=chain_id, 
            signature_type=1, 
            funder=funder
        )
        print("[+] Client object initialized successfully")
        
        # 1. ping
        ok = client.get_ok()
        print(f"[+] Connected to mainnet API. Status = {ok}")

        # 2. Derive/Set API Credentials (CRITICAL for V2 level 2 functions)
        try:
            print("[*] Deriving API Credentials for Level 2 endpoints...")
            creds = client.create_or_derive_api_creds()
            client.set_api_creds(creds)
            print("[+] API Credentials derived successfully")
        except Exception as e:
            print(f"[-] Failed to derive API Credentials: {e}")
            return
            
        address = client.get_address()
        print(f"[+] Wallet Address verified: {address}")
        
        # 3. Fetch Trades
        try:
            print("[*] Requesting historical trades...")
            trades = client.get_trades(TradeParams(maker_address=address))
            trade_list = trades if isinstance(trades, list) else trades.get('data', [])
            print(f"[+] Successfully retrieved trades history. Count: {len(trade_list)}")
            if trade_list:
                print(f"    Latest trade: {trade_list[0].get('side')} on market {trade_list[0].get('asset_id')}")
        except Exception as e:
            print(f"[-] get_trades failed: {e}")
            
        # 4. Fetch Orders
        try:
            print("[*] Requesting open orders...")
            orders = client.get_orders()
            print(f"[+] Successfully retrieved open orders. Count: {len(orders)}")
        except Exception as e:
            print(f"[-] get_orders failed: {e}")
            
        print("\nAll tests completed. If get_trades and get_orders succeeded, the bot is V2 compatible.")

    except Exception as e:
        print("\n[FATAL ERROR] Client setup failed:")
        traceback.print_exc()

if __name__ == '__main__':
    run_tests()
