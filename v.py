import datetime
import json
import logging
import os
import queue
import re
import threading
import uuid
import glob
import time
import shutil
import atexit
import sqlite3
import secrets
from functools import wraps
from urllib.parse import quote
from collections import defaultdict

# Added requests for Spotify API
import requests
# Disable SSL warnings for cleaner logs
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

try:
    import yt_dlp
    from flask import (Flask, Response, jsonify, request, send_from_directory, abort, stream_with_context)
except ImportError:
    print("CRITICAL: Libraries missing. Please run: pip install -r requirements.txt")
    exit(1)

# --- CONFIGURATION ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'database.db')

# Folders
app.config['TEMP_FOLDER'] = os.path.join(DATA_DIR, 'temp')
app.config['DOWNLOAD_FOLDER'] = os.path.join(DATA_DIR, 'downloads')
app.config['TRANSIENT_FOLDER'] = os.path.join(app.config['TEMP_FOLDER'], 'transient')
app.config['COOKIES_FOLDER'] = os.path.join(DATA_DIR, 'cookies')

# Create Directories
for folder in [DATA_DIR, app.config['DOWNLOAD_FOLDER'], app.config['TEMP_FOLDER'], app.config['TRANSIENT_FOLDER'], app.config['COOKIES_FOLDER']]:
    os.makedirs(folder, exist_ok=True)

# ==========================================
#  ### CLEANUP LOGIC ###
# ==========================================

def cleanup_old_files():
    """Background task to delete files older than 1 hour to save space."""
    while True:
        try:
            now = time.time()
            # 1 hour in seconds
            cutoff = now - (60 * 60)
            
            for folder in [app.config['DOWNLOAD_FOLDER'], app.config['TRANSIENT_FOLDER']]:
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if os.path.isfile(file_path):
                        if os.path.getmtime(file_path) < cutoff:
                            logging.info(f"Cleaning up old file: {filename}")
                            os.remove(file_path)
        except Exception as e:
            logging.error(f"Cleanup Error: {e}")
        
        # Run cleanup every 15 minutes
        time.sleep(15 * 60)

# Start the cleanup thread
cleanup_thread = threading.Thread(target=cleanup_old_files, daemon=True)
cleanup_thread.start()

# ==========================================
#  ### CACHE & COOKIES ###
# ==========================================

SPOTIFY_CACHE = {}
CACHE_TTL = 300  # 5 minutes

MODEL_COOKIES = {
    'hotstar': r"""[{"domain":"www.hotstar.com","expirationDate":1768425004.14223,"hostOnly":true,"httpOnly":false,"name":"geo","path":"/in/shows/pakdam-pakdai/1971003171/freaky-fridge/1271514308","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"IN,DL,NEWDELHI,28.60,77.20,55836"},{"domain":"www.hotstar.com","expirationDate":1768511346.914174,"hostOnly":true,"httpOnly":false,"name":"sessionUserUP","path":"/in","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJoSWRcIjpcImIyYmE3OThiMTA3ODQzZDE5MzA0NjQ3MDUxNTE2NTEwXCIsXCJwSWRcIjpcIjYyN2JkZjRkZjYyOTQyZmViYmEwODFkNjFiMzRhZDM4XCIsXCJkd0hpZFwiOlwiZTIyZjE4MzRiMGI2OTAxNGJkZTczN2E1ZmNjMjYzYWQ2MmRiZjRhMjg0MTFhZjYyNTIzMjY0OGU2OGY4OWQ4OFwiLFwiZHdQaWRcIjpcImM0NjAyNzUwMmQ3NDc3Y2IzZWZlNjYyMTc1MGIxYmFiOWZlMWIxM2VlODkxMDJiNjUyODJiODIwMGVhMzljYjBcIixcIm9sZEhpZFwiOlwiYjJiYTc5OGIxMDc4NDNkMTkzMDQ2NDcwNTE1MTY1MTBcIixcIm9sZFBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcImlzUGlpVXNlck1pZ3JhdGVkXCI6ZmFsc2UsXCJuYW1lXCI6XCJTb3VyYXYgXCIsXCJwaG9uZVwiOlwiOTIyOTg0NDUzM1wiLFwiaXBcIjpcIjI0MDk6NDBlNToxMWY0OjdjODo4MDAwOjpcIixcImNvdW50cnlDb2RlXCI6XCJpblwiLFwiY3VzdG9tZXJUeXBlXCI6XCJudVwiLFwidHlwZVwiOlwicGhvbmVcIixcImlzRW1haWxWZXJpZmllZFwiOmZhbHNlLFwiaXNQaG9uZVZlcmlmaWVkXCI6dHJ1ZSxcImRldmljZUlkXCI6XCI0ZmFlZTctMmVkOTkzLTdlZGVkMy04M2Y5MDVcIixcInByb2ZpbGVcIjpcIkFEVUxUXCIsXCJ2ZXJzaW9uXCI6XCJ2MlwiLFwic3Vic2NyaXB0aW9uc1wiOntcImluXCI6e1wiU2luZ2xlRGV2aWNlXCI6e1wic3RhdHVzXCI6XCJTXCIsXCJleHBpcnlcIjpcIjIwMjYtMDMtMDVUMTc6MTk6MjEuMDAwWlwiLFwic2hvd0Fkc1wiOlwiMVwiLFwiY250XCI6XCIxXCJ9fX0sXCJlbnRcIjpcIkN2RUJDZ1VLQXdvQkFCTG5BUklIWVc1a2NtOXBaQklEYVc5ekVnTjNaV0lTQ1dGdVpISnZhV1IwZGhJR1ptbHlaWFIyRWdkaGNIQnNaWFIyRWdSdGQyVmlFZ2QwYVhwbGJuUjJFZ1YzWldKdmN4SUdhbWx2YzNSaUVnUnliMnQxRWdkcWFXOHRiSGxtRWdwamFISnZiV1ZqWVhOMEVnUjBkbTl6RWdSd1kzUjJFZ05xYVc4U0JIaGliM2dTQzNCc1lYbHpkR0YwYVc5dUVnWnJaWEJzWlhJU0RHcHBiM0JvYjI1bGJHbDBaUklOWm1WaGRIVnlaVzF2WW1sc1pSb0NjMlFhQW1oa0dnTm1hR1FhQWpScklnTnpaSElxQm5OMFpYSmxieW9JWkc5c1luazFMakVxQ21SdmJHSjVRWFJ0YjNOWUFRclVBUW9GQ2dNS0FRVVN5Z0VTQjJGdVpISnZhV1FTQTJsdmN4SURkMlZpRWdsaGJtUnliMmxrZEhZU0JtWnBjbVYwZGhJSFlYQndiR1YwZGhJRWJYZGxZaElIZEdsNlpXNTBkaElGZDJWaWIzTVNCbXBwYjNOMFloSUVjbTlyZFJJSGFtbHZMV3g1WmhJS1kyaHliMjFsWTJGemRCSUVkSFp2Y3hJRWNHTjBkaElEYW1sdkVnUjRZbTk0RWd0d2JHRjVjM1JoZEdsdmJoSUdhMlZ3YkdWeUdnSnpaQm9DYUdRYUEyWm9aQm9DTkdzaUEzTmtjaW9HYzNSbGNtVnZLZ2hrYjJ4aWVUVXVNU29LWkc5c1lubEJkRzF2YzFnQkNnMFNDd2dCT0FGQUFWRHdFRmdCQ2hvS0Vnb0FDZzRTQlRVMU9ETTJFZ1UyTkRBME9SSUVPR1JZQVJKN0NBRVFxTE9MK01zekdrZ0tIa3BwYnk1SlRpNVRhVzVuYkdWRVpYWnBZMlV1U1ZCTVRXOXVkR2hzZVJJTVUybHVaMnhsUkdWMmFXTmxHZ05LYVc4Z2tMdXkyUDR5S0tpemkvakxNekFHT0FGQTBDTW9BVEFCT2lBS0hFaHZkSE4wWVhKUWNtVnRhWFZ0TGtsT0xqTk5iMjUwYUM0ME9Ua1FBVWdCXCIsXCJpc3N1ZWRBdFwiOjE3Njg0MjM5OTQ4MDgsXCJtYXR1cml0eUxldmVsXCI6XCJBXCIsXCJpbWdcIjpcIjM4XCIsXCJkcGlkXCI6XCI2MjdiZGY0ZGY2Mjk0MmZlYmJhMDgxZDYxYjM0YWQzOFwiLFwic3RcIjoxLFwiZGF0YVwiOlwiQ2dRSUFDb0FDZ1FJQUJJQUNnUUlBRUlBQ2dRSUFEb0FDZ1FJQURJQUNoSUlBQ0lPZ0FFVWlBRUJrQUhJdThpYWh6ST1cIn0iLCJpc3MiOiJVTSIsImV4cCI6MTc2ODUxMDM5NCwianRpIjoiODkzOGI4ZWQyMDY4NDZiYzlkN2QxNWE2YWFkYjNjOTAiLCJpYXQiOjE3Njg0MjM5OTQsImFwcElkIjoiIiwidGVuYW50IjoiIiwidmVyc2lvbiI6IjFfMCIsImF1ZCI6InVtX2FjY2VzcyJ9.SlEm-BqlF6rCJt5tkt7ExxT8GwgybcadExHHD929h8c"},{"domain":"www.hotstar.com","expirationDate":1802984944.716317,"hostOnly":true,"httpOnly":false,"name":"SELECTED__LANGUAGE","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"eng"},{"domain":"www.hotstar.com","expirationDate":1802984946.77606,"hostOnly":true,"httpOnly":false,"name":"deviceId","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"4faee7-2ed993-7eded3-83f905"},{"domain":"www.hotstar.com","expirationDate":1802984945.876018,"hostOnly":true,"httpOnly":false,"name":"x-hs-setproxystate-ud","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"loc"},{"domain":".hotstar.com","expirationDate":1772799338,"hostOnly":false,"httpOnly":false,"name":"_gcl_au","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"1.1.1713456640.1765023338"},{"domain":".hotstar.com","expirationDate":1802984949.14973,"hostOnly":false,"httpOnly":false,"name":"_ga","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GA1.1.615010706.1765023339"},{"domain":".hotstar.com","expirationDate":1776200947,"hostOnly":false,"httpOnly":false,"name":"_fbp","path":"/","sameSite":"lax","secure":false,"session":false,"storeId":"0","value":"fb.1.1765023339610.656289074171067377"},{"domain":"www.hotstar.com","expirationDate":1799864902,"hostOnly":true,"httpOnly":false,"name":"userCountryCode","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"in"},{"domain":"www.hotstar.com","expirationDate":1799959993,"hostOnly":true,"httpOnly":false,"name":"userHID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"b2ba798b107843d19304647051516510"},{"domain":"www.hotstar.com","expirationDate":1799959993,"hostOnly":true,"httpOnly":false,"name":"userPID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"627bdf4df62942febba081d61b34ad38"},{"domain":"www.hotstar.com","expirationDate":1802984945.861653,"hostOnly":true,"httpOnly":false,"name":"loc","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"ELiEoMsGKAMiggJVRaA2AVk%2BdkUgekEl310mVFox3kPZeWKD5GY0NSP88cI2PwKqhUbG6VhklYN4JgTs9XRw0UJyLx78D9wElxbuwA%2FZCQYUAsx6bkM%2BBB1XGX6EziZU1PvyiYZrlexuVSTfsqR32awFMwH6tALlx4A0Cm2CvdcsyI%2Bam2t2c1RRWr9C7IT9UyjU%2FyQh3gBY6dFmMQeQ71W5%2BbupsgCM2Kjjod3R%2BBRYlmEENXQrsPbgfPwy9lSVput4mNTWPCycfvbceMu0GIE4YLmg68jDJ3KQASJkreRM%2Bhk8CcIH%2FreQ7RMxD37rhysbnWWgSSBgp1xkhHVfMnLGh0gmhxouwnVBRus%3D"},{"domain":"www.hotstar.com","expirationDate":1799959993,"hostOnly":true,"httpOnly":false,"name":"userUP","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJoSWRcIjpcImIyYmE3OThiMTA3ODQzZDE5MzA0NjQ3MDUxNTE2NTEwXCIsXCJwSWRcIjpcIjYyN2JkZjRkZjYyOTQyZmViYmEwODFkNjFiMzRhZDM4XCIsXCJkd0hpZFwiOlwiZTIyZjE4MzRiMGI2OTAxNGJkZTczN2E1ZmNjMjYzYWQ2MmRiZjRhMjg0MTFhZjYyNTIzMjY0OGU2OGY4OWQ4OFwiLFwiZHdQaWRcIjpcImM0NjAyNzUwMmQ3NDc3Y2IzZWZlNjYyMTc1MGIxYmFiOWZlMWIxM2VlODkxMDJiNjUyODJiODIwMGVhMzljYjBcIixcIm9sZEhpZFwiOlwiYjJiYTc5OGIxMDc4NDNkMTkzMDQ2NDcwNTE1MTY1MTBcIixcIm9sZFBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcImlzUGlpVXNlck1pZ3JhdGVkXCI6ZmFsc2UsXCJuYW1lXCI6XCJTb3VyYXYgXCIsXCJwaG9uZVwiOlwiOTIyOTg0NDUzM1wiLFwiaXBcIjpcIjI0MDk6NDBlNToxMWY0OjdjODo4MDAwOjpcIixcImNvdW50cnlDb2RlXCI6XCJpblwiLFwiY3VzdG9tZXJUeXBlXCI6XCJudVwiLFwidHlwZVwiOlwicGhvbmVcIixcImlzRW1haWxWZXJpZmllZFwiOmZhbHNlLFwiaXNQaG9uZVZlcmlmaWVkXCI6dHJ1ZSxcImRldmljZUlkXCI6XCI0ZmFlZTctMmVkOTkzLTdlZGVkMy04M2Y5MDVcIixcInByb2ZpbGVcIjpcIkFEVUxUXCIsXCJ2ZXJzaW9uXCI6XCJ2MlwiLFwic3Vic2NyaXB0aW9uc1wiOntcImluXCI6e1wiU2luZ2xlRGV2aWNlXCI6e1wic3RhdHVzXCI6XCJTXCIsXCJleHBpcnlcIjpcIjIwMjYtMDMtMDVUMTc6MTk6MjEuMDAwWlwiLFwic2hvd0Fkc1wiOlwiMVwiLFwiY250XCI6XCIxXCJ9fX0sXCJlbnRcIjpcIkN2RUJDZ1VLQXdvQkFCTG5BUklIWVc1a2NtOXBaQklEYVc5ekVnTjNaV0lTQ1dGdVpISnZhV1IwZGhJR1ptbHlaWFIyRWdkaGNIQnNaWFIyRWdSdGQyVmlFZ2QwYVhwbGJuUjJFZ1YzWldKdmN4SUdhbWx2YzNSaUVnUnliMnQxRWdkcWFXOHRiSGxtRWdwamFISnZiV1ZqWVhOMEVnUjBkbTl6RWdSd1kzUjJFZ05xYVc4U0JIaGliM2dTQzNCc1lYbHpkR0YwYVc5dUVnWnJaWEJzWlhJU0RHcHBiM0JvYjI1bGJHbDBaUklOWm1WaGRIVnlaVzF2WW1sc1pSb0NjMlFhQW1oa0dnTm1hR1FhQWpScklnTnpaSElxQm5OMFpYSmxieW9JWkc5c1luazFMakVxQ21SdmJHSjVRWFJ0YjNOWUFRclVBUW9GQ2dNS0FRVVN5Z0VTQjJGdVpISnZhV1FTQTJsdmN4SURkMlZpRWdsaGJtUnliMmxrZEhZU0JtWnBjbVYwZGhJSFlYQndiR1YwZGhJRWJYZGxZaElIZEdsNlpXNTBkaElGZDJWaWIzTVNCbXBwYjNOMFloSUVjbTlyZFJJSGFtbHZMV3g1WmhJS1kyaHliMjFsWTJGemRCSUVkSFp2Y3hJRWNHTjBkaElEYW1sdkVnUjRZbTk0RWd0d2JHRjVjM1JoZEdsdmJoSUdhMlZ3YkdWeUdnSnpaQm9DYUdRYUEyWm9aQm9DTkdzaUEzTmtjaW9HYzNSbGNtVnZLZ2hrYjJ4aWVUVXVNU29LWkc5c1lubEJkRzF2YzFnQkNnMFNDd2dCT0FGQUFWRHdFRmdCQ2hvS0Vnb0FDZzRTQlRVMU9ETTJFZ1UyTkRBME9SSUVPR1JZQVJKN0NBRVFxTE9MK01zekdrZ0tIa3BwYnk1SlRpNVRhVzVuYkdWRVpYWnBZMlV1U1ZCTVRXOXVkR2hzZVJJTVUybHVaMnhsUkdWMmFXTmxHZ05LYVc4Z2tMdXkyUDR5S0tpemkvakxNekFHT0FGQTBDTW9BVEFCT2lBS0hFaHZkSE4wWVhKUWNtVnRhWFZ0TGtsT0xqTk5iMjUwYUM0ME9Ua1FBVWdCXCIsXCJpc3N1ZWRBdFwiOjE3Njg0MjM5OTQ4MDgsXCJtYXR1cml0eUxldmVsXCI6XCJBXCIsXCJpbWdcIjpcIjM4XCIsXCJkcGlkXCI6XCI2MjdiZGY0ZGY2Mjk0MmZlYmJhMDgxZDYxYjM0YWQzOFwiLFwic3RcIjoxLFwiZGF0YVwiOlwiQ2dRSUFDb0FDZ1FJQUJJQUNnUUlBRUlBQ2dRSUFEb0FDZ1FJQURJQUNoSUlBQ0lPZ0FFVWlBRUJrQUhJdThpYWh6ST1cIn0iLCJpc3MiOiJVTSIsImV4cCI6MTc2ODUxMDM5NCwianRpIjoiODkzOGI4ZWQyMDY4NDZiYzlkN2QxNWE2YWFkYjNjOTAiLCJpYXQiOjE3Njg0MjM5OTQsImFwcElkIjoiIiwidGVuYW50IjoiIiwidmVyc2lvbiI6IjFfMCIsImF1ZCI6InVtX2FjY2VzcyJ9.SlEm-BqlF6rCJt5tkt7ExxT8GwgybcadExHHD929h8c"},{"domain":".www.hotstar.com","hostOnly":false,"httpOnly":false,"name":"seo-referrer","path":"/","sameSite":"lax","secure":true,"session":true,"storeId":"0","value":""},{"domain":"www.hotstar.com","hostOnly":true,"httpOnly":false,"name":"appLaunchCounter","path":"/","sameSite":"unspecified","secure":false,"session":true,"storeId":"0","value":"1"},{"domain":"www.hotstar.com","expirationDate":1768424986.914064,"hostOnly":true,"httpOnly":false,"name":"AK_SERVER_TIME","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"1768424947"},{"domain":".hotstar.com","expirationDate":1768511347,"hostOnly":false,"httpOnly":false,"name":"_uetsid","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"86193040f0ad11f0a10c3f239a0f94fe"},{"domain":".hotstar.com","expirationDate":1802120947,"hostOnly":false,"httpOnly":false,"name":"_uetvid","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"47209d00d29d11f0a5396f3bea18486f"},{"domain":".hotstar.com","expirationDate":1802984949.2014,"hostOnly":false,"httpOnly":false,"name":"_ga_2PV8LWETCX","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768423994$o11$g1$t1768424949$j59$l0$h0"},{"domain":".hotstar.com","expirationDate":1802984949.213722,"hostOnly":false,"httpOnly":false,"name":"_ga_EPJ8DYH89Z","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768423994$o11$g1$t1768424949$j60$l0$h0"},{"domain":".hotstar.com","expirationDate":1802984949.234664,"hostOnly":false,"httpOnly":false,"name":"_ga_QV5FD29XJC","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768423993$o11$g1$t1768424949$j60$l0$h0"}]""",
    'zee5': r"""""",
    'sonyliv': r"""""",
    'instagram': r"""""",
    'twitter': r"""""",
    'reddit': r"""""",
    'ytdownload': r""""""
}

def setup_cookies():
    """Creates cookie files from MODEL_COOKIES. Automatic Netscape conversion."""
    for model, content in MODEL_COOKIES.items():
        if content and len(content.strip()) > 10:
            file_path = os.path.join(app.config['COOKIES_FOLDER'], f"{model}.txt")
            content = content.strip()
            
            if content.startswith('[') or content.startswith('{'):
                try:
                    cookie_list = json.loads(content)
                    if isinstance(cookie_list, dict): cookie_list = [cookie_list]
                    netscape_lines = ["# Netscape HTTP Cookie File"]
                    for c in cookie_list:
                        domain = c.get('domain', '')
                        flag = 'TRUE' if domain.startswith('.') else 'FALSE'
                        cookie_path_val = c.get('path', '/') 
                        secure = 'TRUE' if c.get('secure') else 'FALSE'
                        exp_val = c.get('expirationDate') or c.get('expiry') or 0
                        expiration = str(int(float(exp_val)))
                        name = c.get('name', '')
                        value = c.get('value', '')
                        line = f"{domain}\t{flag}\t{cookie_path_val}\t{secure}\t{expiration}\t{name}\t{value}"
                        netscape_lines.append(line)
                    final_content = "\n".join(netscape_lines)
                    logging.info(f"Converted JSON cookies to Netscape format for {model}")
                except Exception as e:
                    logging.error(f"Failed to convert cookies for {model}: {e}")
                    final_content = content
            else:
                final_content = content

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(final_content)

setup_cookies()

# --- SUPPORTED MODELS ---
SUPPORTED_MODELS = {
    'ytdownload': [r'youtube\.com', r'youtu\.be'],
    'zee5': [r'zee5\.com'],
    'hotstar': [r'hotstar\.com'],
    'sonyliv': [r'sonyliv\.com'],
    'twitter': [r'twitter\.com', r'x\.com'],
    'instagram': [r'instagram\.com'],
    'reddit': [r'reddit\.com'],
    'spotify': [r'spotify\.com', r'open\.spotify\.com'],
    'generic': []
}

# Browser Headers for API calls
API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
}

# --- SECURITY ---
MASTER_KEY = os.environ.get('MASTER_KEY', 'admin-secret-123')

def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users 
                     (id INTEGER PRIMARY KEY, api_key TEXT UNIQUE, created_at TEXT, is_active INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS access_logs 
                     (id INTEGER PRIMARY KEY, api_key TEXT, ip_address TEXT, endpoint TEXT, model_used TEXT, timestamp TEXT)''')
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"DB Init Error: {e}")

init_db()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        if not api_key: return jsonify({'status': 'error', 'message': 'Missing x-api-key header'}), 401
        conn = get_db_connection()
        user = conn.execute('SELECT * FROM users WHERE api_key = ? AND is_active = 1', (api_key,)).fetchone()
        if not user:
            conn.close()
            return jsonify({'status': 'error', 'message': 'Invalid API Key'}), 403
        model_used = 'unknown'
        if request.is_json: model_used = request.json.get('model', 'generic')
        try:
            timestamp = datetime.datetime.now().isoformat()
            conn.execute('INSERT INTO access_logs (api_key, ip_address, endpoint, model_used, timestamp) VALUES (?, ?, ?, ?, ?)',
                         (api_key, request.remote_addr, request.endpoint, model_used, timestamp))
            conn.commit()
        except Exception: pass
        finally: conn.close()
        return f(*args, **kwargs)
    return decorated_function

def require_master_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        req_key = request.headers.get('x-master-key')
        if req_key != MASTER_KEY:
             return jsonify({'status': 'error', 'message': 'Invalid Master Key'}), 403
        return f(*args, **kwargs)
    return decorated_function

# --- TASK STORE ---
TASKS_STORE = {}

def update_task(tid, data):
    if tid in TASKS_STORE:
        TASKS_STORE[tid]['data'].update(data)
        try: TASKS_STORE[tid]['q'].put(json.dumps(data))
        except: pass

def validate_model(url, model):
    if model not in SUPPORTED_MODELS: return False, f"Invalid model. Supported: {list(SUPPORTED_MODELS.keys())}"
    if model == 'generic': return True, "OK"
    for p in SUPPORTED_MODELS[model]:
        if re.search(p, url, re.IGNORECASE): return True, "OK"
    return False, f"URL does not match model '{model}'"

def detect_model_auto(url, current_model):
    if current_model != 'generic': return current_model
    for model_name, patterns in SUPPORTED_MODELS.items():
        if model_name == 'generic': continue
        for p in patterns:
            if re.search(p, url, re.IGNORECASE): return model_name
    return 'generic'

def clean_ansi(text):
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', str(text)).strip()

def sanitize_filename(name):
    name = re.sub(r'[\\/*?:"<>|#%]', "", name) 
    return name.strip()

def get_formatted_filename(info, model):
    title = info.get('title', 'Unknown')
    series = info.get('series')
    season = info.get('season_number')
    episode = info.get('episode_number')
    
    model_map = {
        'ytdownload': 'YouTube', 'hotstar': 'Hotstar', 'zee5': 'ZEE5',
        'sonyliv': 'SonyLIV', 'instagram': 'Instagram', 'twitter': 'Twitter', 'reddit': 'Reddit',
        'spotify': 'Spotify'
    }
    model_tag = model_map.get(model, model.upper())
    
    if series and season is not None and episode is not None:
        try: base_name = f"{series} - S{int(season):02d}E{int(episode):02d} - {title}"
        except: base_name = title
    else:
        base_name = title
        
    final_name = f"{base_name} [{model_tag}] WEB-DL"
    return sanitize_filename(final_name)

def filter_formats(formats):
    unique_res = {}
    for f in formats:
        if f.get('vcodec') == 'none': continue 
        height = f.get('height')
        if not height: continue
        res_key = f"{height}p"
        f_tbr = f.get('tbr') or 0
        f_id = f['format_id']
        f_ext = f.get('ext')
        is_video_only = 1 if f.get('acodec') == 'none' else 0
        
        if res_key not in unique_res:
             unique_res[res_key] = {'id': f_id, 'resolution': res_key, 'ext': f_ext, 'tbr': f_tbr, 'is_video_only': is_video_only}
             continue

        current = unique_res[res_key]
        if is_video_only and not current['is_video_only']:
            unique_res[res_key] = {'id': f_id, 'resolution': res_key, 'ext': f_ext, 'tbr': f_tbr, 'is_video_only': is_video_only}
            continue
        if is_video_only == current['is_video_only']:
            if f_tbr > current['tbr']:
                 unique_res[res_key] = {'id': f_id, 'resolution': res_key, 'ext': f_ext, 'tbr': f_tbr, 'is_video_only': is_video_only}
    return sorted(unique_res.values(), key=lambda x: int(x['resolution'].replace('p','')), reverse=True)

def filter_audio(formats):
    id_counts = defaultdict(int)
    for f in formats:
        if f.get('vcodec') != 'none' or f.get('acodec') == 'none': continue
        id_counts[f['format_id']] += 1

    best_by_lang = {}
    lang_map = { 'hin': 'hi', 'mal': 'ml', 'tam': 'ta', 'tel': 'te', 'kan': 'kn', 'ben': 'bn', 'mar': 'mr', 'guj': 'gu', 'pan': 'pa' }

    for f in formats:
        if f.get('vcodec') != 'none' or f.get('acodec') == 'none': continue
        raw_lang = f.get('language') or 'Unknown'
        lang = raw_lang 
        abr = f.get('abr') or 0
        fid = f['format_id']
        is_unique_id = (id_counts[fid] == 1)

        if lang not in best_by_lang: should_update = True
        else:
             current = best_by_lang[lang]
             current_is_unique = (id_counts[current['id']] == 1)
             if is_unique_id and not current_is_unique: should_update = True
             elif not is_unique_id and current_is_unique: should_update = False
             else: should_update = abr > current['bitrate']

        if should_update:
            note = f.get('format_note') or ''
            ext = f.get('ext') or ''
            display_lang = lang_map.get(lang, lang)
            label = f"{display_lang} ({int(abr)}kbps)" if abr else f"{display_lang}"
            if note: label += f" - {note}"
            best_by_lang[lang] = {'id': lang, 'language': display_lang, 'bitrate': abr, 'ext': ext, 'label': label}
            
    return sorted(best_by_lang.values(), key=lambda x: (x['language'] == 'Unknown', -x['bitrate']))

# --- CENTRALIZED DOWNLOADER OPTIONS ---
def get_downloader_opts(model):
    """
    Returns the standard Options dictionary with Headers that trick 
    Hotstar/Zee5 into thinking we are a real browser to avoid 475/500 errors.
    """
    opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True, # Important for some SSL configs
        'geo_bypass': True,
        'geo_bypass_country': 'IN', # Force India region
    }

    # Standard Chrome Headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Dest': 'document',
    }

    # Model specific enhancements
    if model == 'hotstar':
        headers.update({
            'Referer': 'https://www.hotstar.com/',
            'Origin': 'https://www.hotstar.com'
        })
        # Hotstar throttles if concurrency is high
        opts['concurrent_fragment_downloads'] = 1
        opts['extractor_args'] = {'hotstar': {'min_timestamp': [0]}}

    elif model == 'zee5':
        headers.update({
            'Referer': 'https://www.zee5.com/',
            'Origin': 'https://www.zee5.com'
        })
    elif model == 'sonyliv':
        headers.update({
            'Referer': 'https://www.sonyliv.com/',
            'Origin': 'https://www.sonyliv.com'
        })

    opts['http_headers'] = headers
    
    # Attach Cookies if they exist
    cookie_path = os.path.join(app.config['COOKIES_FOLDER'], f"{model}.txt")
    if os.path.exists(cookie_path):
        opts['cookiefile'] = cookie_path
        
    return opts

# --- SPOTIFY HELPER ---
def extract_spotify_data(raw_data):
    download_link = None
    title = 'Spotify Track'
    artist = ''
    album_name = ''
    cover = None

    if isinstance(raw_data, dict):
        download_link = raw_data.get('download_url') or raw_data.get('link') or raw_data.get('url')
        
        if 'track_info' in raw_data and isinstance(raw_data['track_info'], dict):
            info = raw_data['track_info']
            title = info.get('name', title)
            artists_list = info.get('artists', [])
            if isinstance(artists_list, list):
                names = [a.get('name', '') for a in artists_list if isinstance(a, dict) and 'name' in a]
                artist = ", ".join(names)
            else:
                artist = str(artists_list)
            album_obj = info.get('album', {})
            if isinstance(album_obj, dict):
                album_name = album_obj.get('name', '')
                images = album_obj.get('images', [])
                if images and isinstance(images, list):
                    first_img = images[0]
                    if isinstance(first_img, dict): cover = first_img.get('url')
        else:
            data = raw_data.get('data') or raw_data
            if isinstance(data, dict):
                title = data.get('title') or data.get('name') or title
                artist_raw = data.get('artist') or data.get('artists')
                if isinstance(artist_raw, list): artist = ", ".join([str(a) for a in artist_raw])
                elif artist_raw: artist = str(artist_raw)
                album_name = data.get('album', '')
                cover = data.get('cover') or data.get('image') or data.get('thumbnail')
                if not download_link: download_link = data.get('link') or data.get('url') or data.get('download_url')

    return download_link, title, artist, album_name, cover

# --- WORKER ---

def single_downloader_core(url, model, format_id, audio_id, tid, cancel_event):
    if model == 'spotify':
        update_task(tid, {'status': 'starting', 'message': 'Processing Spotify Link...'})
        api_url = 'https://spotify-athrix.up.railway.app/sp/dl'
        download_link = None
        title = 'Spotify Track'
        artist = ''
        
        try:
            cached = SPOTIFY_CACHE.get(url)
            if cached and (time.time() - cached['ts'] < CACHE_TTL):
                logging.info(f"Using cached Spotify link for: {url}")
                download_link = cached['link']
                title = cached['meta']['title']
                artist = cached['meta']['artist']
            
            if not download_link:
                update_task(tid, {'status': 'starting', 'message': 'Fetching from Spotify API...'})
                resp = requests.get(api_url, params={'url': url}, headers=API_HEADERS, timeout=30, verify=False)
                resp.raise_for_status()
                raw_data = resp.json()
                download_link, title, artist, _, _ = extract_spotify_data(raw_data)
            
            if artist: title = f"{artist} - {title}"
            if not download_link: raise Exception(f"API did not return a valid link.")
            
            final_filename = sanitize_filename(f"{title} [Spotify] WEB-DL")
            ext = 'mp3'
            
            if cancel_event.is_set(): raise Exception("Cancelled")
            update_task(tid, {'status': 'downloading', 'message': f'Downloading: {final_filename}.{ext}', 'filename': final_filename})
            
            temp_path = os.path.join(app.config['TRANSIENT_FOLDER'], f"{final_filename}.{ext}")
            with requests.get(download_link, stream=True, headers=API_HEADERS, timeout=60, verify=False) as r:
                r.raise_for_status()
                total_length = r.headers.get('content-length')
                with open(temp_path, 'wb') as f:
                    if total_length is None:
                        f.write(r.content)
                    else:
                        dl = 0
                        total_length = int(total_length)
                        for chunk in r.iter_content(chunk_size=8192):
                            if cancel_event.is_set(): raise Exception("Cancelled")
                            if chunk:
                                dl += len(chunk)
                                f.write(chunk)
                                done = int(100 * dl / total_length)
                                update_task(tid, {'status': 'downloading', 'progress': done, 'message': f'Downloading {done}%'})
            return temp_path
        except Exception as e:
            raise Exception(f"Spotify API Error: {str(e)}")

    # ==========================
    #  FIXED YOUTUBE-DL HANDLER
    # ==========================
    
    # 1. GET CENTRALIZED OPTIONS (HEADERS)
    opts = get_downloader_opts(model)

    # 2. CONFIGURE FORMATS
    opts['merge_output_format'] = 'mp4'

    if format_id and audio_id:
        req_format = f"{format_id}+bestaudio[language={audio_id}]"
        merge_msg = f"Merging Video {format_id} + Audio Language {audio_id}"
    elif format_id:
        req_format = f"{format_id}+bestaudio/best"
        merge_msg = f"Merging Video {format_id} + Best Audio"
    elif audio_id:
        req_format = f"bestaudio[language={audio_id}]"
        merge_msg = f"Audio Only: {audio_id}"
    else:
        req_format = 'best'
        merge_msg = "Best Available"

    opts['format'] = req_format

    update_task(tid, {'status':'starting', 'message': f'Fetching Metadata... ({merge_msg})'})

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        raise Exception(f"Metadata Error: {str(e)}")

    if cancel_event.is_set(): raise Exception("Cancelled")

    final_filename = get_formatted_filename(info, model)
    update_task(tid, {'status':'downloading', 'message': f'Starting: {final_filename}', 'filename': final_filename})

    opts['outtmpl'] = os.path.join(app.config['TRANSIENT_FOLDER'], f"{final_filename}.%(ext)s")

    def hook(d):
        if cancel_event.is_set(): raise Exception("Cancelled")
        if d['status'] == 'downloading':
            p_str = clean_ansi(d.get('_percent_str', '0%')).replace('%','')
            try: p = float(p_str)
            except: p = 0
            update_task(tid, {'status':'downloading', 'progress': p, 'message': f'Downloading ({merge_msg})', 'speed': clean_ansi(d.get('_speed_str', '0B/s')), 'eta': clean_ansi(d.get('_eta_str', '00:00'))})

    opts['progress_hooks'] = [hook]

    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
    
    search_pattern = os.path.join(app.config['TRANSIENT_FOLDER'], glob.escape(final_filename) + "*")
    hits = glob.glob(search_pattern)
    
    return hits[0] if hits else None

def worker_single(tid, url, model, format_id, audio_id, cancel_event):
    try:
        path = single_downloader_core(url, model, format_id, audio_id, tid, cancel_event)
        if path:
            filename = os.path.basename(path)
            final_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
            if os.path.exists(final_path):
                name, ext = os.path.splitext(filename)
                final_path = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{name}_{int(time.time())}{ext}")
            shutil.move(path, final_path)
            final_name = os.path.basename(final_path)
            safe_name = quote(final_name)
            dl_link = f"/file/{safe_name}"
            update_task(tid, {'status': 'finished', 'message': 'Ready', 'progress': 100, 'download_url': dl_link, 'filename': final_name})
        else:
            update_task(tid, {'status':'error', 'message': 'File not found on server after download.'})
    except Exception as e:
        update_task(tid, {'status':'error', 'message': str(e)})

# --- ROUTES ---

@app.route('/')
def index():
    return jsonify({'status': 'online', 'supported_models': list(SUPPORTED_MODELS.keys())})

@app.route('/models')
def list_models():
    return jsonify({'models': list(SUPPORTED_MODELS.keys())})

@app.route('/file/<path:filename>')
def serve_file(filename):
    file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found. It may have been cleaned up after 1 hour.'}), 404
    return send_from_directory(app.config['DOWNLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/get-formats', methods=['POST'])
@require_api_key
def get_formats():
    d = request.json
    url = d.get('url')
    model = d.get('model', 'generic').lower()
    model = detect_model_auto(url, model)
    is_valid, msg = validate_model(url, model)
    if not is_valid: return jsonify({'error': msg}), 400
    
    if model == 'spotify':
        try:
            api_url = 'https://spotify-athrix.up.railway.app/sp/dl'
            resp = requests.get(api_url, params={'url': url}, headers=API_HEADERS, timeout=15, verify=False)
            resp.raise_for_status()
            raw_data = resp.json()
            download_link, title, artist, album_name, cover = extract_spotify_data(raw_data)
            full_title = f"{artist} - {title}" if artist else title
            if download_link:
                SPOTIFY_CACHE[url] = {'link': download_link, 'meta': {'title': title, 'artist': artist}, 'ts': time.time()}
            formats = [{'id': 'best', 'resolution': 'Best Quality (MP3)', 'ext': 'mp3', 'tbr': 320, 'note': 'Spotify High Quality'}]
            return jsonify({'status': 'success', 'title': full_title, 'thumbnail': cover, 'description': f"Album: {album_name}" if album_name else "", 'formats': formats, 'audio': []})
        except Exception as e:
            return jsonify({'status': 'success', 'title': f"Spotify Error: {str(e)}", 'formats': [{'id': 'default', 'resolution': 'Standard Audio', 'ext': 'mp3', 'tbr': 128}], 'audio': []})

    # FIX: Use Centralized Options here to prevent 475/500 errors
    opts = get_downloader_opts(model)
        
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return jsonify({
                'status': 'success',
                'title': info.get('title'),
                'thumbnail': info.get('thumbnail'),
                'duration': info.get('duration'),
                'formats': filter_formats(info.get('formats', [])),
                'audio': filter_audio(info.get('formats', []))
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/start-download', methods=['POST'])
@require_api_key
def start_download():
    d = request.json
    url = d.get('url')
    model = d.get('model', 'generic').lower()
    format_id = d.get('format_id')
    audio_id = d.get('audio_id')
    model = detect_model_auto(url, model)
    is_valid, msg = validate_model(url, model)
    if not is_valid: return jsonify({'error': msg}), 400

    tid = uuid.uuid4().hex
    evt = threading.Event()
    TASKS_STORE[tid] = {'q': queue.Queue(), 'event': evt, 'data': {'status': 'starting', 'progress': 0, 'model': model}}
    t = threading.Thread(target=worker_single, args=(tid, url, model, format_id, audio_id, evt), daemon=True)
    t.start()
    return jsonify({'status': 'ok', 'task_id': tid})

@app.route('/stream-progress/<tid>')
@require_api_key
def stream_progress(tid):
    task = TASKS_STORE.get(tid)
    if not task: return jsonify({'error':'Not found'}), 404
    def gen():
        yield f"data: {json.dumps(task['data'])}\n\n"
        while True:
            try:
                data = task['q'].get(timeout=2)
                yield f"data: {data}\n\n"
            except queue.Empty:
                if task['data'].get('status') in ['finished','error']: break
                yield f"data: {json.dumps({'status':'keep-alive'})}\n\n"
    return Response(gen(), mimetype='text/event-stream')

# --- ADMIN ROUTES ---
@app.route('/admin/generate-key', methods=['POST'])
@require_master_key
def generate_key():
    new_key = secrets.token_urlsafe(32)
    conn = get_db_connection()
    conn.execute('INSERT INTO users (api_key, created_at, is_active) VALUES (?, ?, 1)', (new_key, datetime.datetime.now().isoformat()))
    conn.commit()
    conn.close()
    return jsonify({'status': 'success', 'new_api_key': new_key})

@app.route('/admin/db/tables')
@require_master_key
def list_tables():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        return jsonify(tables)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/admin/db/query/<table_name>')
@require_master_key
def query_table(table_name):
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name): return jsonify({'error': 'Invalid table name'}), 400
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        names = [description[0] for description in cursor.description]
        data = []
        for row in rows: data.append(dict(row))
        return jsonify({'columns': names, 'rows': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/admin/db/delete/<table_name>/<int:id>', methods=['DELETE'])
@require_master_key
def delete_row_endpoint(table_name, id):
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name): return jsonify({'error': 'Invalid table name'}), 400
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (id,))
        conn.commit()
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)
