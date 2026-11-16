# -*- coding: utf-8 -*-
"""
ALERT ORDER BLOCK MULTI-TIMEFRAME -> Telegram   (Gate.io perpetual USDT, jalan di CMD)
Hanya alert, tidak membuka posisi. Hanya candle yang sudah tertutup dipakai untuk membentuk dorongan dan OB.

PASANGAN: 4 jam -> 15 menit,  1 jam -> 5 menit.

TF BESAR (4 jam / 1 jam):
  Dorongan : salah satu dari dua jalur (ATR14 dihitung SEBELUM dorongan, ukuran diukur dari BODY),
             dan harus menembus swing sebelumnya (BOS):
             JALUR 1 = 3 candle beruntun searah, total body >= 2,5 x ATR dan minimal satu candle >= 1,5 x ATR
                       (candle lain tidak punya syarat ukuran).
             JALUR 2 = SATU candle searah dengan body >= 1,5 x ATR yang close-nya menembus swing.
  OB       : candle berlawanan terakhir sebelum dorongan, zona dari low sampai high candle itu.
  Umur     : maksimal 100 candle TF besar. OB GUGUR kalau ada candle close menembus zonanya.
TF KECIL (15 menit / 5 menit):
  Dorongan : satu gerakan searah (tidak harus beruntun, maksimal 15 candle) yang menembus swing sebelumnya,
             dan "kuat": ada satu candle searah body >= 1,2 x ATR, ATAU jumlah body candle searah >= 2 x ATR.
  OB       : candle berlawanan terakhir sebelum gerakan itu mulai, zona low - high.
  Syarat   : OB kecil terbentuk SETELAH harga masuk zona OB besar, dan berada di dalam / menyentuh zona OB besar.
ALERT      : saat harga RETEST OB kecil (pertama kali menyentuh sisi dekatnya sesudah dorongan selesai).
             Satu alert per OB kecil. Stop = ujung luar OB kecil -/+ 0,5 x ATR TF kecil. Target RR 1,5 (informasi).

Cara pakai:  JALANKAN_ALERT_OB_MTF.bat (alert terus-menerus)  |  CEK_OB_MTF.bat (scan sekali, tampil di layar)
Token dan chat id ada di config.txt (jangan dibagikan).
"""
import bisect
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)
CONFIG = os.path.join(BASE, "config.txt")
SEEN = os.path.join(BASE, "ob_mtf_sudah_terkirim.json")
HTF_CACHE = os.path.join(BASE, "ob_mtf_cache_htf.json")

# ====================== PENGATURAN (boleh diubah) ======================
PASANGAN = [("4h", "15m"), ("1h", "5m")]   # (TF besar, TF kecil)
ATR_LEN = 14
HTF_TOTAL_ATR = 2.5        # TF besar, jalur 3 candle: jumlah body ketiga candle minimal sekian x ATR
HTF_KUAT_ATR = 1.5         # TF besar: minimal satu candle sekian x ATR (jalur 3 candle) / candle tunggal (jalur 1 candle)
HTF_JALUR_SATU_CANDLE = True   # aktifkan jalur 2: satu candle besar yang menembus swing
SWEEP_MUNDUR = 8           # info sapu likuiditas: dicari sekian candle ke belakang dari OB
VERSI_CACHE = 2            # naikkan bila aturan deteksi berubah (cache lama dibuang)
LTF_KUAT_ATR = 1.2         # TF kecil: satu candle dorongan minimal sekian x ATR (tanda volume bergerak)
LTF_TOTAL_ATR = 2.0        # TF kecil: ATAU jumlah body candle searah dalam gerakan minimal sekian x ATR
UMUR_OB_BESAR = 100        # umur maksimal OB TF besar (dalam candle TF besar)
LTF_MAKS_GERAKAN = 15      # TF kecil: panjang gerakan maksimal (candle)
SL_BUFFER_ATR = 0.5        # stop = ujung luar OB kecil -/+ sekian x ATR TF kecil
RR = 1.5                   # target (informasi di alert)
PIVOT = 3                  # swing = pivot 3 candle kiri dan kanan
CARI_OB_MUNDUR = 6         # mencari candle berlawanan maksimal sekian candle ke belakang
RECENT_LTF = 2             # retest dianggap baru bila terjadi di sekian candle TF kecil terakhir
GUGUR_PAKAI_TF_KECIL = True   # OB besar juga gugur kalau candle TF KECIL close menembus zonanya
JUMLAH_KOIN = 123
RECENT_GITHUB = 8          # mode GitHub: retest dianggap baru bila dalam sekian candle TF kecil terakhir (jadwal GitHub bisa telat)
CANDLE_BESAR = 300         # jumlah candle TF besar yang diambil
CANDLE_KECIL = 1000        # jumlah candle TF kecil yang diambil
DELAY_DETIK = 15           # tunggu sesudah candle tutup supaya data bursa lengkap
INTERVAL_SCAN = 60         # scan tiap 60 detik (versi CMD)
REFRESH_KOIN_JAM = 6
SIMPAN_HARI = 14
WORKERS = 5
# =======================================================================

API = "https://api.gateio.ws/api/v4/futures/usdt"
DETIK_TF = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
WIB = timezone(timedelta(hours=7))

BUANG_BAWAAN = {
    "BMNR_USDT", "MSTR_USDT", "COIN_USDT", "TSLA_USDT", "NVDA_USDT", "AAPL_USDT",
    "HOOD_USDT", "CRCL_USDT", "SBET_USDT",
    "ANTHROPIC_USDT", "OPENAI_USDT", "SPACEX_USDT", "SPCX_USDT",
    "STRIPE_USDT", "DATABRICKS_USDT", "PERPLEXITY_USDT",
}
KATA_NON_KRIPTO = ("stock", "equit", "metal", "index", "indice", "forex", "fx",
                   "commodit", "tradfi", "etf", "gold", "silver")

sesi = requests.Session()
sesi.headers["User-Agent"] = "Mozilla/5.0"


def log(msg):
    print(f"[{datetime.now():%d-%b %H:%M:%S}] {msg}", flush=True)


def jam_wib(ts):
    return datetime.fromtimestamp(ts, WIB).strftime("%d-%m %H:%M")


# ------------------------------------------------------------------ config + Telegram
def baca_config():
    cfg = {}
    if os.path.exists(CONFIG):
        for ln in open(CONFIG, encoding="utf-8"):
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, v = ln.split("=", 1)
                cfg[k.strip().upper()] = v.strip()
    return cfg


def simpan_chat_id(chat_id):
    lines, ada = [], False
    if os.path.exists(CONFIG):
        for ln in open(CONFIG, encoding="utf-8").read().splitlines():
            if ln.strip().upper().startswith("CHAT_ID"):
                lines.append(f"CHAT_ID={chat_id}")
                ada = True
            else:
                lines.append(ln)
    if not ada:
        lines.append(f"CHAT_ID={chat_id}")
    open(CONFIG, "w", encoding="utf-8").write("\n".join(lines) + "\n")


def api(token, method, **data):
    r = requests.post(f"https://api.telegram.org/bot{token}/{method}", data=data, timeout=20)
    return r.json()


def ambil_chat_id(token):
    j = api(token, "getUpdates")
    if not j.get("ok"):
        return None, f"Telegram menolak token ({j.get('description')}). Cek TOKEN di config.txt."
    for u in reversed(j.get("result", [])):
        m = u.get("message") or u.get("channel_post")
        if m and m.get("chat", {}).get("id"):
            return str(m["chat"]["id"]), None
    return None, "Belum ada pesan ke bot. Buka bot di Telegram, tekan START, kirim 'halo', lalu jalankan lagi."


def kirim(token, chat_id, teks):
    for _ in range(3):
        try:
            j = api(token, "sendMessage", chat_id=chat_id, text=teks[:4000], disable_web_page_preview="true")
            if j.get("ok"):
                return True
            log(f"Telegram gagal: {j.get('description')}")
        except Exception as e:
            log(f"Telegram error: {type(e).__name__}")
        time.sleep(3)
    return False


# ------------------------------------------------------------------ data Gate.io
def kontrak_non_kripto():
    try:
        r = sesi.get(f"{API}/contracts", timeout=20)
        r.raise_for_status()
        return {k["name"] for k in r.json()
                if str(k.get("contract_type") or "").lower()
                and any(w in str(k.get("contract_type")).lower() for w in KATA_NON_KRIPTO)}
    except Exception:
        return set()


def ambil_tickers():
    r = sesi.get(f"{API}/tickers", timeout=20)
    r.raise_for_status()
    return r.json()


def daftar_koin(tickers, jumlah=JUMLAH_KOIN):
    data = [d for d in tickers if d.get("contract", "").endswith("_USDT")]
    buang = BUANG_BAWAAN | kontrak_non_kripto()
    data = [d for d in data if d["contract"] not in buang]
    data.sort(key=lambda d: float(d.get("volume_24h_quote") or 0), reverse=True)
    return [d["contract"] for d in data[:jumlah]]


def ambil_candle(kontrak, interval, limit):
    """Return (rows, n_tertutup). rows = (t, o, h, l, c) urut lama -> baru; candle terakhir bisa masih berjalan."""
    step = DETIK_TF[interval]
    k = None
    for p in range(4):
        try:
            r = sesi.get(f"{API}/candlesticks",
                         params={"contract": kontrak, "interval": interval, "limit": limit}, timeout=25)
            if r.status_code == 429:
                time.sleep(2 + 2 * p)
                continue
            r.raise_for_status()
            k = r.json()
            break
        except Exception:
            time.sleep(1 + p)
    if not k:
        raise RuntimeError("tidak ada data")
    rows = sorted(((int(float(x["t"])), float(x["o"]), float(x["h"]), float(x["l"]), float(x["c"])) for x in k))
    now = time.time()
    n_tertutup = sum(1 for c in rows if c[0] + step <= now)
    return rows, n_tertutup


# ------------------------------------------------------------------ indikator
def atr_series(H, L, C, n=ATR_LEN):
    m = len(C)
    tr = [H[0] - L[0]] + [max(H[i] - L[i], abs(H[i] - C[i - 1]), abs(L[i] - C[i - 1])) for i in range(1, m)]
    out = [None] * m
    if m < n:
        return out
    out[n - 1] = sum(tr[:n]) / n
    for i in range(n, m):
        out[i] = (out[i - 1] * (n - 1) + tr[i]) / n
    return out


def pivots(H, L, k=PIVOT):
    ph, pl = [], []
    for p in range(k, len(H) - k):
        if H[p] > max(H[p - k:p]) and H[p] >= max(H[p + 1:p + k + 1]):
            ph.append(p)
        if L[p] < min(L[p - k:p]) and L[p] <= min(L[p + 1:p + k + 1]):
            pl.append(p)
    return ph, pl


def pivot_sebelum(lst, batas):
    """Indeks pivot terbesar yang <= batas, atau None."""
    i = bisect.bisect_right(lst, batas) - 1
    return lst[i] if i >= 0 else None


# ------------------------------------------------------------------ deteksi: TF BESAR
def deteksi_htf(rows, n_closed, step):
    """Dorongan 3 candle + OB pada TF besar (hanya candle tertutup)."""
    T = [c[0] for c in rows[:n_closed]]
    O = [c[1] for c in rows[:n_closed]]
    H = [c[2] for c in rows[:n_closed]]
    L = [c[3] for c in rows[:n_closed]]
    C = [c[4] for c in rows[:n_closed]]
    n = len(C)
    if n < ATR_LEN + 12:
        return []
    atr = atr_series(H, L, C)
    ph, pl = pivots(H, L)
    obs, ada = [], set()
    for i in range(ATR_LEN + 4, n):
        for arah in (1, -1):
            kandidat = []          # (indeks awal dorongan, jalur, kelipatan ATR terbesar)
            # JALUR 1: 3 candle beruntun searah, total body besar, tembus swing
            a, b, c = i - 2, i - 1, i
            ref3 = atr[i - 3]
            idx = (a, b, c)
            if ref3 and ((arah == 1 and all(C[x] > O[x] for x in idx)) or (arah == -1 and all(C[x] < O[x] for x in idx))):
                body = [abs(C[x] - O[x]) for x in idx]
                if sum(body) >= HTF_TOTAL_ATR * ref3 and max(body) >= HTF_KUAT_ATR * ref3:
                    batas = (i - 3) - PIVOT
                    if arah == 1:
                        p = pivot_sebelum(ph, batas)
                        if p is not None and C[i - 3] <= H[p] and max(C[x] for x in idx) > H[p]:
                            kandidat.append((a, "3 candle", max(body) / ref3))
                    else:
                        p = pivot_sebelum(pl, batas)
                        if p is not None and C[i - 3] >= L[p] and min(C[x] for x in idx) < L[p]:
                            kandidat.append((a, "3 candle", max(body) / ref3))
            # JALUR 2: satu candle besar yang close-nya menembus swing
            if HTF_JALUR_SATU_CANDLE:
                ref1 = atr[i - 1]
                if ref1 and ((arah == 1 and C[i] > O[i]) or (arah == -1 and C[i] < O[i])):
                    bd = abs(C[i] - O[i])
                    if bd >= HTF_KUAT_ATR * ref1:
                        batas = (i - 1) - PIVOT
                        if arah == 1:
                            p = pivot_sebelum(ph, batas)
                            if p is not None and C[i] > H[p] and C[i - 1] <= H[p]:
                                kandidat.append((i, "1 candle", bd / ref1))
                        else:
                            p = pivot_sebelum(pl, batas)
                            if p is not None and C[i] < L[p] and C[i - 1] >= L[p]:
                                kandidat.append((i, "1 candle", bd / ref1))
            for awal, jalur, mult in kandidat:
                j = None
                for jj in range(awal - 1, max(awal - 1 - CARI_OB_MUNDUR, -1), -1):
                    if (arah == 1 and C[jj] < O[jj]) or (arah == -1 and C[jj] > O[jj]):
                        j = jj
                        break
                if j is None or (arah, j) in ada:
                    continue
                ada.add((arah, j))
                # info: sapu likuiditas tepat sebelum OB / dorongan
                sweep = False
                for x in range(max(j - SWEEP_MUNDUR, PIVOT + 1), awal):
                    if arah == 1:
                        pk = pivot_sebelum(pl, x - PIVOT)
                        if pk is not None and L[x] < L[pk] and C[x] >= L[pk]:
                            sweep = True
                            break
                    else:
                        pk = pivot_sebelum(ph, x - PIVOT)
                        if pk is not None and H[x] > H[pk] and C[x] <= H[pk]:
                            sweep = True
                            break
                obs.append({"arah": arah, "i": i, "j": j, "lo": L[j], "hi": H[j], "t_j": T[j], "t_i": T[i],
                            "form": T[i] + step, "mult": mult, "jalur": jalur, "sweep": sweep})
    return obs


def status_htf(obs, rows, n_closed, step):
    """Tandai OB aktif (umur, gugur) dan waktu sentuh pertama dari candle TF besar."""
    H = [c[2] for c in rows[:n_closed]]
    L = [c[3] for c in rows[:n_closed]]
    C = [c[4] for c in rows[:n_closed]]
    T = [c[0] for c in rows[:n_closed]]
    n = len(C)
    aktif = []
    for ob in obs:
        if n - 1 - ob["i"] > UMUR_OB_BESAR:
            continue
        gugur, sentuh = False, None
        for q in range(ob["i"] + 1, n):
            bull = ob["arah"] == 1
            kena = (L[q] <= ob["hi"]) if bull else (H[q] >= ob["lo"])
            if kena and sentuh is None:
                sentuh = T[q]
            if (bull and C[q] < ob["lo"]) or ((not bull) and C[q] > ob["hi"]):
                gugur = True
                break
        if not gugur:
            ob = dict(ob)
            ob["sentuh_htf"] = sentuh
            aktif.append(ob)
    return aktif


# ------------------------------------------------------------------ deteksi: TF KECIL
def deteksi_ltf(rows, n_closed, step):
    """Gerakan dorongan + OB kecil pada TF kecil (hanya candle tertutup)."""
    T = [c[0] for c in rows[:n_closed]]
    O = [c[1] for c in rows[:n_closed]]
    H = [c[2] for c in rows[:n_closed]]
    L = [c[3] for c in rows[:n_closed]]
    C = [c[4] for c in rows[:n_closed]]
    n = len(C)
    if n < ATR_LEN + 20:
        return []
    atr = atr_series(H, L, C)
    ph, pl = pivots(H, L)
    out, ada = [], set()
    for k in range(ATR_LEN + 6, n):
        for arah in (1, -1):
            batas = k - 1 - PIVOT
            if arah == 1:
                p = pivot_sebelum(ph, batas)
                if p is None:
                    continue
                S = H[p]
                if not (C[k] > S and C[k - 1] <= S):
                    continue
            else:
                p = pivot_sebelum(pl, batas)
                if p is None:
                    continue
                S = L[p]
                if not (C[k] < S and C[k - 1] >= S):
                    continue
            awal = max(k - LTF_MAKS_GERAKAN + 1, p, 1)
            seg = range(awal, k + 1)
            s = min(seg, key=lambda x: L[x]) if arah == 1 else max(seg, key=lambda x: H[x])
            ref = atr[s - 1]
            if not ref:
                continue
            kuat, total = 0.0, 0.0
            for m in range(s, k + 1):
                if (arah == 1 and C[m] > O[m]) or (arah == -1 and C[m] < O[m]):
                    kuat = max(kuat, abs(C[m] - O[m]) / ref)
                    total += abs(C[m] - O[m]) / ref
            if kuat < LTF_KUAT_ATR and total < LTF_TOTAL_ATR:
                continue
            j = None
            for jj in range(s, max(s - 1 - CARI_OB_MUNDUR, -1), -1):
                if (arah == 1 and C[jj] < O[jj]) or (arah == -1 and C[jj] > O[jj]):
                    j = jj
                    break
            if j is None or j >= k or (arah, j) in ada:
                continue
            ada.add((arah, j))
            out.append({"arah": arah, "k": k, "s": s, "j": j, "lo": L[j], "hi": H[j], "t_j": T[j],
                        "t_k": T[k], "form": T[k] + step, "mult": kuat, "atr": atr[k] or ref})
    return out


def waktu_sentuh(rows, ob, htf_step_awal):
    """Candle TF kecil pertama sesudah OB besar terbentuk yang menyentuh zonanya (waktu buka candle)."""
    bull = ob["arah"] == 1
    awal_data = rows[0][0]
    if ob.get("sentuh_htf") is not None and ob["sentuh_htf"] < awal_data:
        return awal_data        # sentuhan terjadi sebelum jendela data TF kecil: semua candle di jendela dianggap sesudahnya
    for c in rows:
        if c[0] < ob["form"]:
            continue
        if (bull and c[3] <= ob["hi"]) or ((not bull) and c[2] >= ob["lo"]):
            return c[0]
    return None


def cari_sinyal_ltf(ob, rows, n_closed, kecil, htf_tf, ltf_tf, recent):
    """Sinyal retest OB kecil di dalam satu OB besar. Return list sinyal."""
    n_total = len(rows)
    bull = ob["arah"] == 1
    step_l = DETIK_TF[ltf_tf]
    t_touch = waktu_sentuh(rows, ob, DETIK_TF[htf_tf])
    if t_touch is None:
        return []
    if GUGUR_PAKAI_TF_KECIL:
        for c in rows[:n_closed]:
            if c[0] < ob["form"]:
                continue
            if (bull and c[4] < ob["lo"]) or ((not bull) and c[4] > ob["hi"]):
                return []
    sinyal = []
    for sm in kecil:
        if sm["arah"] != ob["arah"] or sm["t_j"] < t_touch:
            continue
        if not (sm["lo"] <= ob["hi"] and sm["hi"] >= ob["lo"]):
            continue
        m_retest, valid = None, True
        for m in range(sm["k"] + 1, n_total):
            c = rows[m]
            kena = (c[3] <= sm["hi"]) if bull else (c[2] >= sm["lo"])
            if kena:
                if m < n_closed and ((bull and c[4] < sm["lo"]) or ((not bull) and c[4] > sm["hi"])):
                    valid = False
                m_retest = m
                break
        if m_retest is None or not valid or m_retest < n_total - recent:
            continue
        entry = sm["hi"] if bull else sm["lo"]
        sl = sm["lo"] - SL_BUFFER_ATR * sm["atr"] if bull else sm["hi"] + SL_BUFFER_ATR * sm["atr"]
        risk = (entry - sl) if bull else (sl - entry)
        if risk <= 0:
            continue
        tp = entry + RR * risk if bull else entry - RR * risk
        sinyal.append({"arah": "BUY" if bull else "SELL", "htf": htf_tf, "ltf": ltf_tf,
                       "big_lo": ob["lo"], "big_hi": ob["hi"], "big_t": ob["t_i"] + DETIK_TF[htf_tf],
                       "big_mult": ob["mult"], "big_jalur": ob.get("jalur", "3 candle"), "big_sweep": bool(ob.get("sweep")), "sm_lo": sm["lo"], "sm_hi": sm["hi"], "sm_t": sm["t_k"] + step_l,
                       "sm_key": sm["t_j"], "mult": sm["mult"], "entry": entry, "sl": sl, "tp": tp,
                       "risk_pct": risk / entry * 100, "t_retest": rows[m_retest][0],
                       "umur": n_total - 1 - m_retest, "px": rows[-1][4]})
    return sinyal


# ------------------------------------------------------------------ pesan
def tv_link(label):
    return "https://www.tradingview.com/chart/?symbol=" + quote(f"GATEIO:{label}USDT.P", safe=":")


def fmt(x):
    return "n/a" if x is None else f"{x:.6g}"


def pesan(label, s):
    buy = s["arah"] == "BUY"
    ikon = "🟢" if buy else "🔴"
    jenis = "bullish" if buy else "bearish"
    out = [f"{ikon} {s['arah']} | ORDER BLOCK {s['htf']}→{s['ltf']} | {label}"]
    out.append(f"OB {s['htf']} {jenis}: {fmt(s['big_lo'])} - {fmt(s['big_hi'])} (dorongan selesai {jam_wib(s['big_t'])} WIB, "
               f"dorongan {s['big_jalur']}, candle terbesar {s['big_mult']:.1f} ATR). Harga sudah masuk zona ini.")
    if s.get("big_sweep"):
        out.append("Info: sebelum OB besar ada sapu likuiditas (wick menembus low/high lalu balik).")
    out.append(f"OB {s['ltf']} {jenis}: {fmt(s['sm_lo'])} - {fmt(s['sm_hi'])} (terbentuk {jam_wib(s['sm_t'])} WIB)")
    out.append(f"Volume bergerak: candle dorongan {s['ltf']} terbesar {s['mult']:.1f} ATR")
    out.append(f"RETEST OB {s['ltf']}: {jam_wib(s['t_retest'])} WIB ({s['umur']} candle lalu)")
    out.append(f"Entry di zona: {fmt(s['entry'])}")
    out.append(f"Stop {fmt(s['sl'])} (ujung OB {s['ltf']} {'-' if buy else '+'} {SL_BUFFER_ATR:g} ATR, risiko {s['risk_pct']:.2f}%)")
    out.append(f"Target {fmt(s['tp'])} (RR {RR:g})")
    out.append(f"Harga sekarang {fmt(s['px'])}")
    out.append(f"Chart: {tv_link(label)}")
    return "\n".join(out)


# ------------------------------------------------------------------ riwayat sinyal
def muat_seen():
    try:
        d = json.load(open(SEEN, encoding="utf-8"))
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def simpan_seen(d):
    batas = time.time() - SIMPAN_HARI * 86400
    d = {k: v for k, v in d.items() if v >= batas}
    json.dump(d, open(SEEN, "w", encoding="utf-8"))
    return d


# ------------------------------------------------------------------ satu putaran scan
class Cache:
    def __init__(self):
        self.htf = {}        # (kontrak, htf_tf) -> {"last": waktu candle tertutup terakhir, "obs": [...]}


def htf_perlu_refresh(cache, kontrak, htf_tf, now):
    step = DETIK_TF[htf_tf]
    terakhir_diharapkan = (int(now) // step) * step - step
    e = cache.htf.get((kontrak, htf_tf))
    return e is None or e["last"] < terakhir_diharapkan


def refresh_htf(args):
    kontrak, htf_tf, cache = args
    try:
        rows, n_closed = ambil_candle(kontrak, htf_tf, CANDLE_BESAR)
        step = DETIK_TF[htf_tf]
        obs = status_htf(deteksi_htf(rows, n_closed, step), rows, n_closed, step)
        cache.htf[(kontrak, htf_tf)] = {"last": rows[n_closed - 1][0] if n_closed else 0, "obs": obs}
        return True
    except Exception:
        return False


def evaluasi_koin(args):
    """Evaluasi satu koin untuk satu TF kecil. Return list (label, sinyal) atau None bila gagal ambil data."""
    kontrak, htf_tf, ltf_tf, obs_kandidat, recent = args
    try:
        rows, n_closed = ambil_candle(kontrak, ltf_tf, CANDLE_KECIL)
        kecil = deteksi_ltf(rows, n_closed, DETIK_TF[ltf_tf])
        hasil = []
        for ob in obs_kandidat:
            for s in cari_sinyal_ltf(ob, rows, n_closed, kecil, htf_tf, ltf_tf, recent):
                hasil.append((kontrak[:-5], s))
        return hasil
    except Exception:
        return None


def kandidat_ob(cache, kontrak, htf_tf, harga):
    """OB besar aktif yang sudah tersentuh (dari candle TF besar) atau sedang disentuh harga sekarang."""
    e = cache.htf.get((kontrak, htf_tf))
    if not e:
        return []
    hasil = []
    for ob in e["obs"]:
        bull = ob["arah"] == 1
        live = harga is not None and ((harga <= ob["hi"]) if bull else (harga >= ob["lo"]))
        if ob["sentuh_htf"] is not None or live:
            hasil.append(ob)
    return hasil


def jalan_tick(koin, harga, cache, recent=RECENT_LTF):
    t0 = time.time()
    now = time.time()
    # 1) refresh OB TF besar yang candle-nya baru tutup
    tugas = [(k, htf, cache) for k in koin for htf, _ in PASANGAN if htf_perlu_refresh(cache, k, htf, now)]
    gagal_htf = 0
    if tugas:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for ok in ex.map(refresh_htf, tugas):
                gagal_htf += (not ok)
    # 2) kandidat: koin yang OB besarnya sudah/sedang tersentuh
    kerja = []
    total_ob = tersentuh = 0
    for k in koin:
        for htf, ltf in PASANGAN:
            e = cache.htf.get((k, htf))
            if e:
                total_ob += len(e["obs"])
            kand = kandidat_ob(cache, k, htf, harga.get(k))
            tersentuh += len(kand)
            if kand:
                kerja.append((k, htf, ltf, kand, recent))
    hasil, gagal_ltf = [], 0
    if kerja:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for h in ex.map(evaluasi_koin, kerja):
                if h is None:
                    gagal_ltf += 1
                else:
                    hasil.extend(h)
    info = {"detik": time.time() - t0, "refresh_htf": len(tugas), "gagal_htf": gagal_htf, "total_ob": total_ob,
            "dipantau": tersentuh, "dievaluasi": len(kerja), "gagal_ltf": gagal_ltf}
    return hasil, info


def kunci(label, s):
    return f"{label}|{s['htf']}>{s['ltf']}|{s['arah']}|{s['sm_key']}"


def harga_dari(tickers):
    out = {}
    for d in tickers:
        try:
            out[d["contract"]] = float(d["last"])
        except Exception:
            pass
    return out


# ------------------------------------------------------------------ mode cek (tanpa Telegram)
def mode_cek(argv):
    recent = 12
    nama = None
    for a in argv:
        if a.startswith("recent="):
            recent = int(a[7:])
        else:
            nama = a.upper().replace("_USDT", "")
    print("Mengambil daftar koin dari Gate.io ...")
    tickers = ambil_tickers()
    koin = [nama + "_USDT"] if nama else daftar_koin(tickers)
    harga = harga_dari(tickers)
    print(f"Scan {len(koin)} koin | pasangan {', '.join(h + '->' + l for h, l in PASANGAN)} | retest dalam {recent} candle TF kecil terakhir\n")
    cache = Cache()
    hasil, info = jalan_tick(koin, harga, cache, recent=recent)
    print(f"OB besar aktif: {info['total_ob']} | sudah/sedang disentuh harga: {info['dipantau']} | "
          f"data gagal: {info['gagal_htf'] + info['gagal_ltf']} | {info['detik']:.0f} detik\n")
    # daftar OB besar yang sedang dipantau
    for (k, htf), e in sorted(cache.htf.items()):
        for ob in e["obs"]:
            bull = ob["arah"] == 1
            live = harga.get(k) is not None and ((harga[k] <= ob["hi"]) if bull else (harga[k] >= ob["lo"]))
            if ob["sentuh_htf"] is not None or live:
                print(f"  dipantau: {k[:-5]:<10} OB {htf} {'bullish' if bull else 'bearish'} {fmt(ob['lo'])} - {fmt(ob['hi'])} "
                      f"(selesai {jam_wib(ob['t_i'] + DETIK_TF[htf])} WIB)")
    print()
    if not hasil:
        print("Tidak ada sinyal retest saat ini.")
    for label, s in sorted(hasil, key=lambda x: x[1]["umur"]):
        print(pesan(label, s))
        print()


# ------------------------------------------------------------------ mode GitHub (sekali jalan, token dari GitHub Secrets)
def muat_cache_htf():
    c = Cache()
    try:
        d = json.load(open(HTF_CACHE, encoding="utf-8"))
        if d.get("_v") != VERSI_CACHE:
            return c
        for k, v in d.items():
            if k == "_v":
                continue
            kon, htf = k.split("|")
            c.htf[(kon, htf)] = v
    except Exception:
        pass
    return c


def simpan_cache_htf(c, koin):
    aktif = set(koin)
    d = {f"{k}|{h}": v for (k, h), v in c.htf.items() if k in aktif}
    d["_v"] = VERSI_CACHE
    json.dump(d, open(HTF_CACHE, "w", encoding="utf-8"))


def mode_github():
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID belum diisi di GitHub Secrets.")
        sys.exit(1)
    tickers = ambil_tickers()
    koin = daftar_koin(tickers)
    harga = harga_dari(tickers)
    cache = muat_cache_htf()
    seen = muat_seen()
    pertama = seen is None
    seen = seen or {}
    hasil, info = jalan_tick(koin, harga, cache, recent=RECENT_GITHUB)
    simpan_cache_htf(cache, koin)
    baru = []
    for label, s in hasil:
        key = kunci(label, s)
        if key not in seen:
            seen[key] = time.time()
            baru.append((label, s))
    baru.sort(key=lambda x: x[1]["umur"])
    log(f"scan: OB besar {info['total_ob']} (dipantau {info['dipantau']}), refresh {info['refresh_htf']}, "
        f"{len(hasil)} retest aktif, {len(baru)} baru, gagal {info['gagal_htf'] + info['gagal_ltf']} ({info['detik']:.0f} detik)")
    if pertama:
        kirim(token, chat_id, "✅ Bot alert ORDER BLOCK multi-timeframe aktif di GitHub.\nPasangan: 4 jam → 15 menit, 1 jam → 5 menit.\n"
                              f"Retest yang sudah aktif saat ini ({len(baru)}) tidak dikirim satu-satu; alert berikutnya hanya yang baru.\n"
                              "Catatan: jadwal GitHub bisa telat 5-60 menit, jadi alert bisa terlambat.")
    else:
        for label, s in baru:
            kirim(token, chat_id, pesan(label, s))
            time.sleep(0.4)
    simpan_seen(seen)


# ------------------------------------------------------------------ main
def main():
    if len(sys.argv) > 1 and sys.argv[1].lower() == "github":
        mode_github()
        return
    if len(sys.argv) > 1 and sys.argv[1].lower() == "cek":
        mode_cek(sys.argv[2:])
        return
    cfg = baca_config()
    token = cfg.get("TOKEN", "")
    if not token or "PASTE" in token.upper():
        print("Isi dulu TOKEN di config.txt (buka dengan Notepad), lalu jalankan lagi.")
        sys.exit(2)
    chat_id = cfg.get("CHAT_ID", "")
    try:
        if not chat_id:
            chat_id, err = ambil_chat_id(token)
            if err:
                print(err)
                sys.exit(2)
            simpan_chat_id(chat_id)
            log("Chat ID ditemukan dan disimpan di config.txt")
    except Exception as e:
        print(f"Tidak bisa menghubungi Telegram ({type(e).__name__}). Cek internet.")
        sys.exit(2)

    seen = muat_seen()
    pertama = seen is None
    seen = seen or {}
    koin, kapan_koin = None, 0
    cache = Cache()
    log("Bot alert ORDER BLOCK multi-timeframe mulai. Jangan tutup jendela ini.")
    kirim(token, chat_id, "✅ Bot alert ORDER BLOCK multi-timeframe aktif.\nPasangan: 4 jam → 15 menit, 1 jam → 5 menit (Gate.io perpetual), scan tiap 1 menit.\n"
                          "OB TF besar = dorongan (3 candle total ≥2,5 ATR, atau 1 candle ≥1,5 ATR) yang menembus swing. Harga masuk zona itu, lalu di TF kecil terbentuk dorongan "
                          "(1 candle ≥1,2 ATR atau total ≥2 ATR) + OB kecil di dalam zona besar. Alert bunyi saat harga RETEST OB kecil. "
                          "Stop di ujung OB kecil ∓ 0,5 ATR, target RR 1,5.")
    mulai = True
    while True:
        try:
            if koin is None or time.time() - kapan_koin > REFRESH_KOIN_JAM * 3600:
                try:
                    koin, kapan_koin = daftar_koin(ambil_tickers()), time.time()
                    log(f"{len(koin)} koin dimuat dari Gate.io")
                except Exception as e:
                    if koin is None:
                        log(f"Belum bisa terhubung ke Gate.io ({type(e).__name__}), coba lagi 60 detik.")
                        time.sleep(60)
                        continue
            if not mulai:
                now = time.time()
                nxt = (int(now // INTERVAL_SCAN) + 1) * INTERVAL_SCAN
                time.sleep(max(1, nxt + DELAY_DETIK - now))
            harga = harga_dari(ambil_tickers())
            hasil, info = jalan_tick(koin, harga, cache)
            baru = []
            for label, s in hasil:
                key = kunci(label, s)
                if key not in seen:
                    seen[key] = time.time()
                    baru.append((label, s))
            baru.sort(key=lambda x: x[1]["umur"])
            log(f"scan: OB besar {info['total_ob']} (dipantau {info['dipantau']}), refresh {info['refresh_htf']}, "
                f"{len(hasil)} retest aktif, {len(baru)} baru, gagal {info['gagal_htf'] + info['gagal_ltf']} ({info['detik']:.0f} detik)")
            if mulai:
                mulai = False
                if pertama and baru:
                    kepala = f"Retest OB aktif saat bot dinyalakan ({len(baru)}), tidak dikirim satu-satu:"
                    blok, panjang = [kepala], len(kepala)
                    for label, s in baru:
                        ln = f"{s['arah']} {label} {s['htf']}→{s['ltf']} (retest {jam_wib(s['t_retest'])} WIB)\n{tv_link(label)}"
                        if panjang + len(ln) > 3500:
                            kirim(token, chat_id, "\n".join(blok))
                            blok, panjang = [], 0
                        blok.append(ln)
                        panjang += len(ln) + 1
                    kirim(token, chat_id, "\n".join(blok))
            else:
                for label, s in baru:
                    kirim(token, chat_id, pesan(label, s))
                    time.sleep(0.4)
            seen = simpan_seen(seen)
        except KeyboardInterrupt:
            log("Dihentikan.")
            return
        except Exception as e:
            log(f"Error: {type(e).__name__}: {str(e)[:100]} - lanjut putaran berikutnya")
            time.sleep(30)


if __name__ == "__main__":
    main()
