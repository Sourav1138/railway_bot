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
    'hotstar': r"""[{"domain":"www.hotstar.com","expirationDate":1768543732.014651,"hostOnly":true,"httpOnly":false,"name":"sessionUserUP","path":"/in","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJoSWRcIjpcImIyYmE3OThiMTA3ODQzZDE5MzA0NjQ3MDUxNTE2NTEwXCIsXCJwSWRcIjpcIjYyN2JkZjRkZjYyOTQyZmViYmEwODFkNjFiMzRhZDM4XCIsXCJkd0hpZFwiOlwiZTIyZjE4MzRiMGI2OTAxNGJkZTczN2E1ZmNjMjYzYWQ2MmRiZjRhMjg0MTFhZjYyNTIzMjY0OGU2OGY4OWQ4OFwiLFwiZHdQaWRcIjpcImM0NjAyNzUwMmQ3NDc3Y2IzZWZlNjYyMTc1MGIxYmFiOWZlMWIxM2VlODkxMDJiNjUyODJiODIwMGVhMzljYjBcIixcIm9sZEhpZFwiOlwiYjJiYTc5OGIxMDc4NDNkMTkzMDQ2NDcwNTE1MTY1MTBcIixcIm9sZFBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcImlzUGlpVXNlck1pZ3JhdGVkXCI6ZmFsc2UsXCJuYW1lXCI6XCJTb3VyYXYgXCIsXCJwaG9uZVwiOlwiOTIyOTg0NDUzM1wiLFwiaXBcIjpcIjI0MDk6NDBlNToxMWUwOjVlMDg6ODAwMDo6XCIsXCJjb3VudHJ5Q29kZVwiOlwiaW5cIixcImN1c3RvbWVyVHlwZVwiOlwibnVcIixcInR5cGVcIjpcInBob25lXCIsXCJpc0VtYWlsVmVyaWZpZWRcIjpmYWxzZSxcImlzUGhvbmVWZXJpZmllZFwiOnRydWUsXCJkZXZpY2VJZFwiOlwiNGZhZWU3LTJlZDk5My03ZWRlZDMtODNmOTA1XCIsXCJwcm9maWxlXCI6XCJBRFVMVFwiLFwidmVyc2lvblwiOlwidjJcIixcInN1YnNjcmlwdGlvbnNcIjp7XCJpblwiOntcIlNpbmdsZURldmljZVwiOntcInN0YXR1c1wiOlwiU1wiLFwiZXhwaXJ5XCI6XCIyMDI2LTAzLTA1VDE3OjE5OjIxLjAwMFpcIixcInNob3dBZHNcIjpcIjFcIixcImNudFwiOlwiMVwifX19LFwiZW50XCI6XCJDdkVCQ2dVS0F3b0JBQkxuQVJJSFlXNWtjbTlwWkJJRGFXOXpFZ04zWldJU0NXRnVaSEp2YVdSMGRoSUdabWx5WlhSMkVnZGhjSEJzWlhSMkVnUnRkMlZpRWdkMGFYcGxiblIyRWdWM1pXSnZjeElHYW1sdmMzUmlFZ1J5YjJ0MUVnZHFhVzh0YkhsbUVncGphSEp2YldWallYTjBFZ1IwZG05ekVnUndZM1IyRWdOcWFXOFNCSGhpYjNnU0MzQnNZWGx6ZEdGMGFXOXVFZ1pyWlhCc1pYSVNER3BwYjNCb2IyNWxiR2wwWlJJTlptVmhkSFZ5WlcxdlltbHNaUm9DYzJRYUFtaGtHZ05tYUdRYUFqUnJJZ056WkhJcUJuTjBaWEpsYnlvSVpHOXNZbmsxTGpFcUNtUnZiR0o1UVhSdGIzTllBUXJVQVFvRkNnTUtBUVVTeWdFU0IyRnVaSEp2YVdRU0EybHZjeElEZDJWaUVnbGhibVJ5YjJsa2RIWVNCbVpwY21WMGRoSUhZWEJ3YkdWMGRoSUViWGRsWWhJSGRHbDZaVzUwZGhJRmQyVmliM01TQm1wcGIzTjBZaElFY205cmRSSUhhbWx2TFd4NVpoSUtZMmh5YjIxbFkyRnpkQklFZEhadmN4SUVjR04wZGhJRGFtbHZFZ1I0WW05NEVndHdiR0Y1YzNSaGRHbHZiaElHYTJWd2JHVnlHZ0p6WkJvQ2FHUWFBMlpvWkJvQ05Hc2lBM05rY2lvR2MzUmxjbVZ2S2doa2IyeGllVFV1TVNvS1pHOXNZbmxCZEcxdmMxZ0JDZzBTQ3dnQk9BRkFBVkR3RUZnQkNob0tFZ29BQ2c0U0JUVTFPRE0yRWdVMk5EQTBPUklFT0dSWUFSSjdDQUVRcUxPTCtNc3pHa2dLSGtwcGJ5NUpUaTVUYVc1bmJHVkVaWFpwWTJVdVNWQk1UVzl1ZEdoc2VSSU1VMmx1WjJ4bFJHVjJhV05sR2dOS2FXOGdrTHV5MlA0eUtLaXppL2pMTXpBR09BRkEwQ01vQVRBQk9pQUtIRWh2ZEhOMFlYSlFjbVZ0YVhWdExrbE9Mak5OYjI1MGFDNDBPVGtRQVVnQlwiLFwiaXNzdWVkQXRcIjoxNzY4NDU1OTI5NTI1LFwibWF0dXJpdHlMZXZlbFwiOlwiQVwiLFwiaW1nXCI6XCIzOFwiLFwiZHBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcInN0XCI6MSxcImRhdGFcIjpcIkNnUUlBQ29BQ2dRSUFESUFDZ1FJQURvQUNnUUlBRUlBQ2dRSUFCSUFDaElJQUNJT2dBRVVpQUVCa0FISXU4aWFoekk9XCJ9IiwiaXNzIjoiVU0iLCJleHAiOjE3Njg1NDIzMjksImp0aSI6Ijg5MzhiOGVkMjA2ODQ2YmM5ZDdkMTVhNmFhZGIzYzkwIiwiaWF0IjoxNzY4NDU1OTI5LCJhcHBJZCI6IiIsInRlbmFudCI6IiIsInZlcnNpb24iOiIxXzAiLCJhdWQiOiJ1bV9hY2Nlc3MifQ.tcDXFu6NB7USrlbVg2ZskXXAlqsNCnaEPVcFeoMuOm4"},{"domain":"www.hotstar.com","expirationDate":1803017328.91248,"hostOnly":true,"httpOnly":false,"name":"SELECTED__LANGUAGE","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"eng"},{"domain":"www.hotstar.com","expirationDate":1803017331.701726,"hostOnly":true,"httpOnly":false,"name":"deviceId","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"4faee7-2ed993-7eded3-83f905"},{"domain":"www.hotstar.com","expirationDate":1803017329.934594,"hostOnly":true,"httpOnly":false,"name":"x-hs-setproxystate-ud","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"loc"},{"domain":".hotstar.com","expirationDate":1772799338,"hostOnly":false,"httpOnly":false,"name":"_gcl_au","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"1.1.1713456640.1765023338"},{"domain":".hotstar.com","expirationDate":1803017333.630971,"hostOnly":false,"httpOnly":false,"name":"_ga","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GA1.1.615010706.1765023339"},{"domain":".hotstar.com","expirationDate":1776233331,"hostOnly":false,"httpOnly":false,"name":"_fbp","path":"/","sameSite":"lax","secure":false,"session":false,"storeId":"0","value":"fb.1.1765023339610.656289074171067377"},{"domain":"www.hotstar.com","expirationDate":1799864902,"hostOnly":true,"httpOnly":false,"name":"userCountryCode","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"in"},{"domain":"www.hotstar.com","expirationDate":1799991928,"hostOnly":true,"httpOnly":false,"name":"userHID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"b2ba798b107843d19304647051516510"},{"domain":"www.hotstar.com","expirationDate":1799991928,"hostOnly":true,"httpOnly":false,"name":"userPID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"627bdf4df62942febba081d61b34ad38"},{"domain":"www.hotstar.com","expirationDate":1799991928,"hostOnly":true,"httpOnly":false,"name":"userUP","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJoSWRcIjpcImIyYmE3OThiMTA3ODQzZDE5MzA0NjQ3MDUxNTE2NTEwXCIsXCJwSWRcIjpcIjYyN2JkZjRkZjYyOTQyZmViYmEwODFkNjFiMzRhZDM4XCIsXCJkd0hpZFwiOlwiZTIyZjE4MzRiMGI2OTAxNGJkZTczN2E1ZmNjMjYzYWQ2MmRiZjRhMjg0MTFhZjYyNTIzMjY0OGU2OGY4OWQ4OFwiLFwiZHdQaWRcIjpcImM0NjAyNzUwMmQ3NDc3Y2IzZWZlNjYyMTc1MGIxYmFiOWZlMWIxM2VlODkxMDJiNjUyODJiODIwMGVhMzljYjBcIixcIm9sZEhpZFwiOlwiYjJiYTc5OGIxMDc4NDNkMTkzMDQ2NDcwNTE1MTY1MTBcIixcIm9sZFBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcImlzUGlpVXNlck1pZ3JhdGVkXCI6ZmFsc2UsXCJuYW1lXCI6XCJTb3VyYXYgXCIsXCJwaG9uZVwiOlwiOTIyOTg0NDUzM1wiLFwiaXBcIjpcIjI0MDk6NDBlNToxMWUwOjVlMDg6ODAwMDo6XCIsXCJjb3VudHJ5Q29kZVwiOlwiaW5cIixcImN1c3RvbWVyVHlwZVwiOlwibnVcIixcInR5cGVcIjpcInBob25lXCIsXCJpc0VtYWlsVmVyaWZpZWRcIjpmYWxzZSxcImlzUGhvbmVWZXJpZmllZFwiOnRydWUsXCJkZXZpY2VJZFwiOlwiNGZhZWU3LTJlZDk5My03ZWRlZDMtODNmOTA1XCIsXCJwcm9maWxlXCI6XCJBRFVMVFwiLFwidmVyc2lvblwiOlwidjJcIixcInN1YnNjcmlwdGlvbnNcIjp7XCJpblwiOntcIlNpbmdsZURldmljZVwiOntcInN0YXR1c1wiOlwiU1wiLFwiZXhwaXJ5XCI6XCIyMDI2LTAzLTA1VDE3OjE5OjIxLjAwMFpcIixcInNob3dBZHNcIjpcIjFcIixcImNudFwiOlwiMVwifX19LFwiZW50XCI6XCJDdkVCQ2dVS0F3b0JBQkxuQVJJSFlXNWtjbTlwWkJJRGFXOXpFZ04zWldJU0NXRnVaSEp2YVdSMGRoSUdabWx5WlhSMkVnZGhjSEJzWlhSMkVnUnRkMlZpRWdkMGFYcGxiblIyRWdWM1pXSnZjeElHYW1sdmMzUmlFZ1J5YjJ0MUVnZHFhVzh0YkhsbUVncGphSEp2YldWallYTjBFZ1IwZG05ekVnUndZM1IyRWdOcWFXOFNCSGhpYjNnU0MzQnNZWGx6ZEdGMGFXOXVFZ1pyWlhCc1pYSVNER3BwYjNCb2IyNWxiR2wwWlJJTlptVmhkSFZ5WlcxdlltbHNaUm9DYzJRYUFtaGtHZ05tYUdRYUFqUnJJZ056WkhJcUJuTjBaWEpsYnlvSVpHOXNZbmsxTGpFcUNtUnZiR0o1UVhSdGIzTllBUXJVQVFvRkNnTUtBUVVTeWdFU0IyRnVaSEp2YVdRU0EybHZjeElEZDJWaUVnbGhibVJ5YjJsa2RIWVNCbVpwY21WMGRoSUhZWEJ3YkdWMGRoSUViWGRsWWhJSGRHbDZaVzUwZGhJRmQyVmliM01TQm1wcGIzTjBZaElFY205cmRSSUhhbWx2TFd4NVpoSUtZMmh5YjIxbFkyRnpkQklFZEhadmN4SUVjR04wZGhJRGFtbHZFZ1I0WW05NEVndHdiR0Y1YzNSaGRHbHZiaElHYTJWd2JHVnlHZ0p6WkJvQ2FHUWFBMlpvWkJvQ05Hc2lBM05rY2lvR2MzUmxjbVZ2S2doa2IyeGllVFV1TVNvS1pHOXNZbmxCZEcxdmMxZ0JDZzBTQ3dnQk9BRkFBVkR3RUZnQkNob0tFZ29BQ2c0U0JUVTFPRE0yRWdVMk5EQTBPUklFT0dSWUFSSjdDQUVRcUxPTCtNc3pHa2dLSGtwcGJ5NUpUaTVUYVc1bmJHVkVaWFpwWTJVdVNWQk1UVzl1ZEdoc2VSSU1VMmx1WjJ4bFJHVjJhV05sR2dOS2FXOGdrTHV5MlA0eUtLaXppL2pMTXpBR09BRkEwQ01vQVRBQk9pQUtIRWh2ZEhOMFlYSlFjbVZ0YVhWdExrbE9Mak5OYjI1MGFDNDBPVGtRQVVnQlwiLFwiaXNzdWVkQXRcIjoxNzY4NDU1OTI5NTI1LFwibWF0dXJpdHlMZXZlbFwiOlwiQVwiLFwiaW1nXCI6XCIzOFwiLFwiZHBpZFwiOlwiNjI3YmRmNGRmNjI5NDJmZWJiYTA4MWQ2MWIzNGFkMzhcIixcInN0XCI6MSxcImRhdGFcIjpcIkNnUUlBQ29BQ2dRSUFESUFDZ1FJQURvQUNnUUlBRUlBQ2dRSUFCSUFDaElJQUNJT2dBRVVpQUVCa0FISXU4aWFoekk9XCJ9IiwiaXNzIjoiVU0iLCJleHAiOjE3Njg1NDIzMjksImp0aSI6Ijg5MzhiOGVkMjA2ODQ2YmM5ZDdkMTVhNmFhZGIzYzkwIiwiaWF0IjoxNzY4NDU1OTI5LCJhcHBJZCI6IiIsInRlbmFudCI6IiIsInZlcnNpb24iOiIxXzAiLCJhdWQiOiJ1bV9hY2Nlc3MifQ.tcDXFu6NB7USrlbVg2ZskXXAlqsNCnaEPVcFeoMuOm4"},{"domain":".www.hotstar.com","hostOnly":false,"httpOnly":false,"name":"seo-referrer","path":"/","sameSite":"lax","secure":true,"session":true,"storeId":"0","value":""},{"domain":"www.hotstar.com","hostOnly":true,"httpOnly":false,"name":"appLaunchCounter","path":"/","sameSite":"unspecified","secure":false,"session":true,"storeId":"0","value":"1"},{"domain":"www.hotstar.com","expirationDate":1803017329.92854,"hostOnly":true,"httpOnly":false,"name":"loc","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"EPKIossGKAMi%2FQFV94anFMLZptLaTRCmVpBh%2Fp0I%2FxTz5cWESD3MeggBtiojAByrw3Oy86aDbd0MklBBYY8Wvxm9bK5ZUSX1a9XAwH4NRM%2B7emKSvaFnjEmqnYg0BYJOvfper7Walb6X4T490jQF46fn5vhhGkWmYZv%2B7klRdcg%2BDNLI8MS6FLvwpZlaw%2BOv6hKuyc44HPu%2FqDXuIuPbO9hDxRpeZpRL%2Ft2fGymGksml2rToTqD35d2sWVVw3%2B9ID%2B5mgiIhdKXPN5aBvxce0UbzhMhi9LEmkycEd3ckH9X89C8T0D%2FEq9z1hzDEX9YqCM%2FuGv8746hB3spJkC5MdRD84xuNYexf"},{"domain":".hotstar.com","expirationDate":1768543731,"hostOnly":false,"httpOnly":false,"name":"_uetsid","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"86193040f0ad11f0a10c3f239a0f94fe"},{"domain":".hotstar.com","expirationDate":1802153331,"hostOnly":false,"httpOnly":false,"name":"_uetvid","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"47209d00d29d11f0a5396f3bea18486f"},{"domain":".hotstar.com","expirationDate":1803017332.609349,"hostOnly":false,"httpOnly":false,"name":"_ga_2PV8LWETCX","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768455928$o13$g1$t1768457332$j60$l0$h0"},{"domain":".hotstar.com","expirationDate":1803017333.725405,"hostOnly":false,"httpOnly":false,"name":"_ga_EPJ8DYH89Z","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768455928$o13$g1$t1768457333$j59$l0$h0"},{"domain":".hotstar.com","expirationDate":1803017333.78331,"hostOnly":false,"httpOnly":false,"name":"_ga_QV5FD29XJC","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"GS2.1.s1768455928$o13$g1$t1768457333$j60$l0$h0"}]""",
    'zee5': r"""""",
    'sonyliv': r"""""",
    'instagram': r"""""",
    'twitter': r"""""",
    'reddit': r"""""",
    'ytdownload': r"""[{"domain":".youtube.com","expirationDate":1768459292.686012,"hostOnly":false,"httpOnly":true,"name":"GPS","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"1"},{"domain":".youtube.com","expirationDate":1803017608.294087,"hostOnly":false,"httpOnly":false,"name":"PREF","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"f6=40000000&tz=Asia.Calcutta"},{"domain":".youtube.com","expirationDate":1803017605.313086,"hostOnly":false,"httpOnly":true,"name":"HSID","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"AR-VYYJ2naMyS2eo0"},{"domain":".youtube.com","expirationDate":1803017605.313185,"hostOnly":false,"httpOnly":true,"name":"SSID","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"A5BJ7u8e6sz-LpBvx"},{"domain":".youtube.com","expirationDate":1803017605.313275,"hostOnly":false,"httpOnly":false,"name":"APISID","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"NiNruv_HpV7uXsv1/AV0Yj_ufOnG8JlVAi"},{"domain":".youtube.com","expirationDate":1803017605.313372,"hostOnly":false,"httpOnly":false,"name":"SAPISID","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"vrxUutrs9PnUQbkS/A-UrBkkNcKBzxtlmJ"},{"domain":".youtube.com","expirationDate":1803017605.313467,"hostOnly":false,"httpOnly":false,"name":"__Secure-1PAPISID","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"vrxUutrs9PnUQbkS/A-UrBkkNcKBzxtlmJ"},{"domain":".youtube.com","expirationDate":1803017605.313566,"hostOnly":false,"httpOnly":false,"name":"__Secure-3PAPISID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"vrxUutrs9PnUQbkS/A-UrBkkNcKBzxtlmJ"},{"domain":".youtube.com","expirationDate":1803017605.313664,"hostOnly":false,"httpOnly":false,"name":"SID","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"g.a0005wjmwK-_73FnHYc8utj5xFquyIkNG30DQDjABMYk60GypJOux-SqhXQGChBrIjwDv24JqQACgYKAY0SARASFQHGX2MiCXfCRsG0NLBj657MH2gxnhoVAUF8yKqxMShJYskGKhoqXY9PfgkA0076"},{"domain":".youtube.com","expirationDate":1803017605.31379,"hostOnly":false,"httpOnly":true,"name":"__Secure-1PSID","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"g.a0005wjmwK-_73FnHYc8utj5xFquyIkNG30DQDjABMYk60GypJOup7XvopCqjDFe5vESaQmuCAACgYKAXQSARASFQHGX2MifZj6h0tt3D6pRN6DNXrSkRoVAUF8yKqr9jaOwRARAl-Oz5O6Jchh0076"},{"domain":".youtube.com","expirationDate":1803017605.313904,"hostOnly":false,"httpOnly":true,"name":"__Secure-3PSID","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"g.a0005wjmwK-_73FnHYc8utj5xFquyIkNG30DQDjABMYk60GypJOuPJNu_6VlGpET8Q3x-VnSrgACgYKAbwSARASFQHGX2Mif1_u8Cw6mmB8UxxO_p1hiBoVAUF8yKroGc6ja-HvN9iCD7tmHwXo0076"},{"domain":".youtube.com","expirationDate":1799993605.312704,"hostOnly":false,"httpOnly":true,"name":"__Secure-1PSIDTS","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"sidts-CjUB7I_69CuNPPN8ZAqKPVwgwyMCB6OPUJwOpikHz6af8QQ5AGuOjp6BRCPY-pH60JMDo2QxWhAA"},{"domain":".youtube.com","expirationDate":1799993605.312972,"hostOnly":false,"httpOnly":true,"name":"__Secure-3PSIDTS","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"sidts-CjUB7I_69CuNPPN8ZAqKPVwgwyMCB6OPUJwOpikHz6af8QQ5AGuOjp6BRCPY-pH60JMDo2QxWhAA"},{"domain":".youtube.com","expirationDate":1803017606.958544,"hostOnly":false,"httpOnly":true,"name":"LOGIN_INFO","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"AFmmF2swRAIgG55QRzjWbwyzX_EBMw4MaSzcy-JfoLgqy0yResSTs14CICCo07S_tNTSd9lFkKCjQ144fX96Td6N5OCxtvDkcUU0:QUQ3MjNmektoMUljMUJqWVlvNDdJdTVvbjRuV3NjV3F6VFN0TTF4WG40eU4zU2xTenZfS3NPb19kQVhiRkFmQm1UcjVhVFFoVHF2Z1dybkNUd1djWGJGNFpNUUplQjZRdmJXSFVoWW8tbi1xampRS3RqRGhPS05SdmtkdTNUQlc1QXlOd195el9OYXRMMG4xQ3FIQUE5bnhRcjlobFRxaUxn"},{"domain":".youtube.com","expirationDate":1799993611.384945,"hostOnly":false,"httpOnly":false,"name":"SIDCC","path":"/","sameSite":"unspecified","secure":false,"session":false,"storeId":"0","value":"AKEyXzWqYSuaTq4i69SXELEa0B2d6qysDHbZB-vPGv_ZL76_kOT7U8z10G24zsR88awJNLpGxQ"},{"domain":".youtube.com","expirationDate":1799993611.385089,"hostOnly":false,"httpOnly":true,"name":"__Secure-1PSIDCC","path":"/","sameSite":"unspecified","secure":true,"session":false,"storeId":"0","value":"AKEyXzXzzBaXYeEvxO2GzItmI3VrwwWk01RV5QHz-KPb3-L_eGw4POKJFRPx1vFmPs22Zbb1Yg"},{"domain":".youtube.com","expirationDate":1799993611.385195,"hostOnly":false,"httpOnly":true,"name":"__Secure-3PSIDCC","path":"/","sameSite":"no_restriction","secure":true,"session":false,"storeId":"0","value":"AKEyXzWPMh8lGBoIxV366dSZtIuG2_oUB5ww6Q7CYnmh7RPpbrfpmwIfSr_763uoqBg8avTgEA"}]"""
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
        
        # Check if any user exists, if not create default '12345' key
        c.execute('SELECT count(*) FROM users')
        if c.fetchone()[0] == 0:
            default_key = "12345"
            c.execute('INSERT INTO users (api_key, created_at, is_active) VALUES (?, ?, 1)', (default_key, datetime.datetime.now().isoformat()))
            logging.warning(f"No API keys found. Created default key: {default_key}")
            
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
        
        # --- AUTO-ALLOW Localhost OR Default Key ---
        if request.remote_addr == '127.0.0.1' or api_key == '12345':
            model_used = 'unknown'
            try:
                if request.is_json:
                     model_used = request.get_json(silent=True).get('model', 'generic')
            except: pass
            
            try:
                conn = get_db_connection()
                timestamp = datetime.datetime.now().isoformat()
                key_to_log = api_key if api_key else 'localhost-bypass'
                conn.execute('INSERT INTO access_logs (api_key, ip_address, endpoint, model_used, timestamp) VALUES (?, ?, ?, ?, ?)',
                             (key_to_log, request.remote_addr, request.endpoint, model_used, timestamp))
                conn.commit()
                conn.close()
            except: pass
            
            return f(*args, **kwargs)
        # ------------------------------------------------
        
        if not api_key: return jsonify({'status': 'error', 'message': 'Missing x-api-key header'}), 401
        conn = get_db_connection()
        user = conn.execute('SELECT * FROM users WHERE api_key = ? AND is_active = 1', (api_key,)).fetchone()
        if not user:
            conn.close()
            return jsonify({'status': 'error', 'message': 'Invalid API Key'}), 403
        
        model_used = 'unknown'
        try:
            if request.is_json:
                model_used = request.get_json(silent=True).get('model', 'generic')
        except: pass
        
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
    if not url: return False, "URL is missing"
    if model not in SUPPORTED_MODELS: return False, f"Invalid model. Supported: {list(SUPPORTED_MODELS.keys())}"
    if model == 'generic': return True, "OK"
    for p in SUPPORTED_MODELS[model]:
        if re.search(p, url, re.IGNORECASE): return True, "OK"
    return False, f"URL does not match model '{model}'"

def detect_model_auto(url, current_model):
    if not url: return 'generic'
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

# --- REVISED FILTERING LOGIC ---

def filter_formats(formats):
    """
    Returns ALL valid video formats (including those with audio) sorted by resolution.
    Accepts formats even without explicit bitrate/codec if resolution/ext exists.
    Fixes OTT issues where codec info might be missing.
    """
    video_formats = []
    seen_ids = set()
    
    for f in formats:
        # Relaxed checks for OTT
        has_video = False
        if f.get('vcodec') and f.get('vcodec') != 'none': has_video = True
        elif f.get('width') or f.get('height'): has_video = True
        elif f.get('ext') in ['mp4', 'mkv', 'webm', 'ts'] and f.get('acodec') != 'none': has_video = True # Assume muxed if audio exists in video container
        
        if not has_video: continue
        
        # Robust height check
        height = f.get('height') or f.get('rows')
        if not height: 
            res = f.get('resolution')
            if res and 'x' in res:
                try: height = int(res.split('x')[1])
                except: height = 0
            else:
                height = 0
        
        fid = f['format_id']
        if fid in seen_ids: continue
        seen_ids.add(fid)
        
        ext = f.get('ext', '')
        fps = f.get('fps')
        tbr = f.get('tbr') or 0
        note = f.get('format_note') or ''
        acodec = f.get('acodec')
        
        # Label Construction
        label_parts = []
        if height > 0:
            label_parts.append(f"{height}p")
        else:
            label_parts.append("Unknown Resolution")
            
        if ext: label_parts.append(f"({ext})")
        if fps: label_parts.append(f"{fps}fps")
        if tbr: label_parts.append(f"{int(tbr)}kbps")
        
        # Distinguish Video Only vs Video+Audio
        if acodec and acodec != 'none':
            label_parts.append("[Video+Audio]")
        else:
            label_parts.append("[Video Only]")
            
        if note: label_parts.append(note)
        
        label = " - ".join(label_parts)
        
        video_formats.append({
            'id': fid,
            'resolution': f"{height}p" if height > 0 else "Unknown",
            'label': label,
            'ext': ext,
            'tbr': tbr,
            'height': height
        })

    # Sort by Height (desc), then Bitrate (desc)
    return sorted(video_formats, key=lambda x: (x['height'], x['tbr']), reverse=True)

def filter_audio(formats):
    """
    Returns ALL valid audio-only formats.
    """
    audio_formats = []
    seen_ids = set()
    
    lang_map = { 'hin': 'Hindi', 'mal': 'Malayalam', 'tam': 'Tamil', 'tel': 'Telugu', 'kan': 'Kannada', 'ben': 'Bengali', 'mar': 'Marathi', 'guj': 'Gujarati', 'pan': 'Punjabi', 'eng': 'English', 'jap': 'Japanese' }

    for f in formats:
        # Must be audio only
        acodec = f.get('acodec', 'none')
        vcodec = f.get('vcodec', 'none')
        
        # Strict check: Audio codec must exist, Video codec must NOT exist
        if acodec == 'none': continue
        if vcodec != 'none': continue
        
        fid = f['format_id']
        if fid in seen_ids: continue
        seen_ids.add(fid)
        
        raw_lang = f.get('language') or 'und'
        display_lang = lang_map.get(raw_lang, raw_lang)
        if display_lang == 'und': display_lang = 'Unknown'
        
        abr = f.get('abr') or 0
        ext = f.get('ext', '')
        note = f.get('format_note') or ''
        
        # Label: "Hindi (128kbps - m4a)"
        label = f"{display_lang} ({int(abr)}kbps - {ext})"
        if note: label += f" [{note}]"
        
        audio_formats.append({
            'id': fid, 
            'language': display_lang,
            'bitrate': abr,
            'ext': ext,
            'label': label
        })
            
    return sorted(audio_formats, key=lambda x: (x['language'], -x['bitrate']))

# --- CENTRALIZED DOWNLOADER OPTIONS ---
def get_downloader_opts(model):
    """
    Returns standard Options.
    """
    opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True, 
        'cachedir': False, # Prevent stale cache issues
    }

    # Base IP Spoof header
    spoof_headers = {
        'X-Forwarded-For': '103.208.220.12'
    }

    if model == 'hotstar':
        opts['concurrent_fragment_downloads'] = 1
        opts['extractor_args'] = {'hotstar': {'min_timestamp': [0]}}
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.hotstar.com/',
            'Origin': 'https://www.hotstar.com',
        }
        headers.update(spoof_headers)
        opts['http_headers'] = headers

    elif model == 'zee5':
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.zee5.com/',
            'Origin': 'https://www.zee5.com',
        }
        headers.update(spoof_headers)
        opts['http_headers'] = headers
        
    elif model == 'ytdownload' or model == 'generic':
        # YOUTUBE STRATEGY:
        # 1. Use 'android' which naturally bypasses geo-blocks (most robust).
        # 2. Use 'ios' as fallback (good quality).
        # 3. DO NOT use 'web' as it causes geo-block errors (403/Unavailable) on server IPs.
        # 4. Inject X-Forwarded-For to assist clients.
        opts['extractor_args'] = {'youtube': {'player_client': ['android', 'ios']}}
        opts['geo_bypass'] = True
        opts['geo_bypass_country'] = 'IN' 
        opts['http_headers'] = spoof_headers 

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
                resp = requests.get(api_url, params={'url': url}, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30, verify=False)
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
            with requests.get(download_link, stream=True, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60, verify=False) as r:
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
    
    opts = get_downloader_opts(model)
    opts['merge_output_format'] = 'mp4'

    # Strict format construction
    if format_id and audio_id:
        req_format = f"{format_id}+{audio_id}"
        merge_msg = f"Merging Video {format_id} + Audio {audio_id}"
    elif format_id:
        # DO NOT append +bestaudio here. Trust the user's ID.
        req_format = f"{format_id}" 
        merge_msg = f"Downloading Format {format_id}"
    elif audio_id:
        req_format = audio_id
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

@app.route('/sw.js')
def service_worker():
    return Response("", mimetype="application/javascript")

@app.route('/file/<path:filename>')
def serve_file(filename):
    file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found. It may have been cleaned up after 1 hour.'}), 404
    return send_from_directory(app.config['DOWNLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/get-formats', methods=['POST'])
@require_api_key
def get_formats():
    d = request.get_json(force=True, silent=True) or {}
    url = d.get('url')
    
    model_raw = d.get('model', 'generic')
    model = model_raw.lower() if isinstance(model_raw, str) else 'generic'
    
    model = detect_model_auto(url, model)
    is_valid, msg = validate_model(url, model)
    if not is_valid: return jsonify({'error': msg}), 400
    
    if model == 'spotify':
        try:
            api_url = 'https://spotify-athrix.up.railway.app/sp/dl'
            resp = requests.get(api_url, params={'url': url}, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15, verify=False)
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
    try:
        d = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({'error': 'Invalid JSON body'}), 400
        
    url = d.get('url')
    
    model_raw = d.get('model', 'generic')
    model = model_raw.lower() if isinstance(model_raw, str) else 'generic'
    
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
