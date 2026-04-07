#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grok Elite Swing v5.0
美股波段選股模型 — S&P 500 + Nasdaq 100
執行方式：python grok_elite_swing.py
輸出：Top10 CSV / ALL CSV / HTML 精美報告（含 Plotly K 線圖）

依賴套件（pip install）：
    yfinance pandas ta numpy requests plotly
"""

# ════════════════════════════════════════════════════════════════
# ① 套件匯入
# ════════════════════════════════════════════════════════════════
import io
import json
import math
import os
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import ta as _ta
import plotly.graph_objects as go
import plotly.io as pio
import requests
import yfinance as yf
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.2f}'.format)

TODAY = datetime.today().strftime('%Y%m%d')
print(f'✅ 套件載入完成 | 執行日期：{TODAY}')


# ════════════════════════════════════════════════════════════════
# ② 抓取 S&P 500 + Nasdaq 100 成分股清單
# ════════════════════════════════════════════════════════════════
WIKI_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def fetch_wiki_tables(url):
    resp = requests.get(url, headers=WIKI_HEADERS, timeout=20)
    resp.raise_for_status()
    return pd.read_html(io.StringIO(resp.text))


def get_sp500_tickers():
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = fetch_wiki_tables(url)
        df = tables[0]
        col = next(
            (c for c in df.columns if 'symbol' in c.lower() or 'ticker' in c.lower()),
            df.columns[0]
        )
        tickers = (
            df[col].dropna()
            .astype(str)
            .str.strip()
            .str.replace(r'\.B$', '-B', regex=True)
            .tolist()
        )
        print(f'✅ S&P 500：抓到 {len(tickers)} 檔')
        return tickers
    except Exception as e:
        print(f'⚠️ S&P 500 抓取失敗：{e}，改用內建清單')
        return []


def get_nasdaq100_tickers():
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = fetch_wiki_tables(url)
        for tbl in tables:
            cols_lower = [str(c).lower() for c in tbl.columns]
            if any('ticker' in c or 'symbol' in c for c in cols_lower):
                col_idx = next(
                    i for i, c in enumerate(cols_lower)
                    if 'ticker' in c or 'symbol' in c
                )
                col = tbl.columns[col_idx]
                tickers = (
                    tbl[col].dropna()
                    .astype(str)
                    .str.strip()
                    .tolist()
                )
                tickers = [t for t in tickers if 1 <= len(t) <= 6 and t != 'nan']
                if len(tickers) >= 50:
                    print(f'✅ Nasdaq-100：抓到 {len(tickers)} 檔')
                    return tickers
        raise ValueError('找不到合適的欄位')
    except Exception as e:
        print(f'⚠️ Nasdaq-100 抓取失敗：{e}，改用內建清單')
        return []


# 完整內建備用清單（Wikipedia 萬一失敗時使用）
BUILTIN_TICKERS = list(set([
    # Mega-cap / Nasdaq 100 核心
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','GOOG','TSLA','AVGO','COST',
    'NFLX','AMD','ADBE','CSCO','INTC','CMCSA','AMGN','TXN','QCOM','INTU',
    'AMAT','ISRG','BKNG','MU','REGN','ADI','PANW','LRCX','KLAC','CDNS',
    'SNPS','ASML','MELI','ABNB','DXCM','IDXX','MRNA','BIIB','VRTX','FAST',
    'ROST','PAYX','AEP','CPRT','ODFL','KHC','DLTR','HON','ON','FANG',
    'FTNT','GEHC','GFS','ILMN','KDP','MDLZ','MNST','MRVL','NXPI',
    'ORLY','PCAR','PYPL','SBUX','SGEN','TEAM','TTD','VRSK','WDAY',
    'XEL','ZM','ZS','CEG','CSGP','DDOG','EA','EXC','FSLR','GILD',
    'ANSS','CHTR','CTSH','ENPH','TTWO','ALGN','AZN','BKR','CCEP',
    # 金融
    'JPM','BAC','WFC','GS','MS','C','USB','PNC','TFC','COF',
    'AXP','BLK','SCHW','CB','MMC','AON','MET','PRU','AFL','AIG',
    'SPGI','MCO','ICE','CME','CBOE','MSCI','FIS','FI','BR',
    # 醫療
    'JNJ','UNH','PFE','ABBV','MRK','LLY','TMO','DHR','ABT','BMY',
    'MDT','SYK','ELV','CVS','CI','HUM','CNC','DGX','LH',
    # 能源
    'XOM','CVX','COP','EOG','SLB','OXY','PSX','VLO','MPC','HES',
    # 公用事業
    'NEE','DUK','SO','D','AEP','SRE','PCG','ED',
    # 科技
    'ORCL','IBM','CRM','SAP','ACN','NOW','SNOW','PLTR','UBER','DASH',
    # 消費
    'WMT','TGT','HD','LOW','TJX','DG','MCD','YUM','PG','KO','PEP',
    'PM','MO','DIS','BA','RTX','LMT','NOC','GD',
    'CAT','DE','EMR','ETN','ITW','GE','HON','UPS','FDX','CSX','UNP',
    'TSLA','GM','F','RIVN',
    # 原物料 / 房地產
    'LIN','APD','NEM','FCX','ALB',
    'AMT','PLD','EQIX','CCI','DLR','PSA',
    # 金融科技
    'V','MA','SQ','AFRM','SOFI','NU','HOOD','COIN','MSTR',
    # 建商
    'DHI','LEN','PHM',
]))


def build_ticker_list():
    sp500  = get_sp500_tickers()
    ndx100 = get_nasdaq100_tickers()
    combined = list(set(sp500 + ndx100))
    if len(combined) < 100:
        print(f'⚠️ Wikipedia 僅抓到 {len(combined)} 檔，補充內建清單...')
        combined = list(set(combined + BUILTIN_TICKERS))
    all_tickers = sorted(set(
        t for t in combined
        if isinstance(t, str)
        and 1 <= len(t) <= 6
        and t not in ('', 'nan', 'NaN')
        and not t.startswith('.')
    ))
    print(f'\n📊 合計股票池：{len(all_tickers)} 檔（去重後）')
    return all_tickers


# ════════════════════════════════════════════════════════════════
# ③ 下載歷史價格資料
# ════════════════════════════════════════════════════════════════
BATCH_SIZE = 100
PERIOD     = '1y'
INTERVAL   = '1d'


def download_batch(tickers, period=PERIOD, interval=INTERVAL):
    result = {}
    try:
        raw = yf.download(
            tickers     = tickers,
            period      = period,
            interval    = interval,
            group_by    = 'ticker',
            auto_adjust = True,
            progress    = False,
            threads     = True
        )
        if len(tickers) == 1:
            t = tickers[0]
            if not raw.empty:
                result[t] = raw
        else:
            for t in tickers:
                try:
                    df = raw[t].dropna(how='all')
                    if len(df) >= 60:
                        result[t] = df
                except Exception:
                    pass
    except Exception as e:
        print(f'  ⚠️ 批次下載錯誤：{e}')
    return result


def download_all(all_tickers):
    print('📥 下載 SPY / QQQ 基準...')
    benchmark_data = download_batch(['SPY', 'QQQ'])
    spy_ret_1m = float('nan')
    qqq_ret_1m = float('nan')

    if 'SPY' in benchmark_data and len(benchmark_data['SPY']) >= 22:
        spy_close  = benchmark_data['SPY']['Close']
        spy_ret_1m = (spy_close.iloc[-1] - spy_close.iloc[-22]) / spy_close.iloc[-22]
        print(f'  SPY 近 1 個月報酬：{spy_ret_1m:.2%}')

    if 'QQQ' in benchmark_data and len(benchmark_data['QQQ']) >= 22:
        qqq_close  = benchmark_data['QQQ']['Close']
        qqq_ret_1m = (qqq_close.iloc[-1] - qqq_close.iloc[-22]) / qqq_close.iloc[-22]
        print(f'  QQQ 近 1 個月報酬：{qqq_ret_1m:.2%}')

    print(f'\n📥 開始分批下載 {len(all_tickers)} 檔股票（每批 {BATCH_SIZE} 檔）...')
    price_data = {}
    batches    = [all_tickers[i:i+BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]

    for idx, batch in enumerate(batches):
        print(f'  批次 {idx+1}/{len(batches)}：下載 {len(batch)} 檔...', end=' ', flush=True)
        batch_result = download_batch(batch)
        price_data.update(batch_result)
        print(f'成功 {len(batch_result)} 檔')
        if idx < len(batches) - 1:
            time.sleep(1.5)

    print(f'\n✅ 資料下載完成：共 {len(price_data)} 檔有效資料')
    return price_data, spy_ret_1m, qqq_ret_1m


# ════════════════════════════════════════════════════════════════
# ④ 計算技術指標 & Grok Elite Score v5.0
# ════════════════════════════════════════════════════════════════
MIN_MARKET_CAP = 10_000_000_000
MIN_PRICE      = 10.0
MIN_AVG_VOL    = 1_000_000
MIN_1M_RETURN  = 0.08


def _nan(v):
    try:
        return math.isnan(float(v))
    except Exception:
        return True


def calc_new_elite_score(latest_close, ema20, vol_ratio, high_20, rsi14, macd_hist):
    """
    v5.0 技術細化評分
    回傳: (new_score, breakdown, cb, pq, vs, rs, tc, bonus)
    """
    pullback_pct = float('nan')
    if (not _nan(high_20)) and float(high_20) > 0:
        pullback_pct = (float(high_20) - float(latest_close)) / float(high_20) * 100

    above_ema20 = (
        (not _nan(latest_close)) and
        (not _nan(ema20)) and
        (float(latest_close) > float(ema20))
    )

    # ── 1. Consolidation Breakout (最高 +18) ────────────────────
    cb = 0
    if above_ema20 and (not _nan(vol_ratio)) and (not _nan(pullback_pct)):
        vr = float(vol_ratio)
        pb = float(pullback_pct)
        if   vr >= 2.0 and pb <= 8:   cb = 18
        elif vr >= 1.5 and pb <= 12:  cb = 12
        elif vr >= 1.2 and pb <= 15:  cb = 8
        elif vr >= 1.0 and pb <= 15:  cb = 4

    # ── 2. Pullback Quality (最高 +15) ──────────────────────────
    pq = 0
    if above_ema20 and (not _nan(pullback_pct)):
        pb = float(pullback_pct)
        if   5  <= pb <= 10: pq = 15
        elif 10 <  pb <= 15: pq = 10
        elif pb > 15:        pq = 5

    # ── 3. Volume Surge (最高 +12) ──────────────────────────────
    vs = 0
    if not _nan(vol_ratio):
        vr = float(vol_ratio)
        if   vr >= 2.0:         vs = 12
        elif 1.5 <= vr <= 1.99: vs = 8
        elif 1.2 <= vr <= 1.49: vs = 4

    # ── 4. Relative Strength (固定 +8) ──────────────────────────
    rs = 8

    # ── 5. Technical Confirmation (最高 +10) ────────────────────
    tc = 0
    if (not _nan(rsi14)) and (not _nan(macd_hist)):
        r  = float(rsi14)
        mh = float(macd_hist)
        rsi_main = 40 <= r <= 65
        rsi_edge = (38 <= r < 40) or (65 < r <= 68)
        macd_pos  = mh > 0
        macd_flat = -0.1 <= mh <= 0
        macd_near = abs(mh) < 0.05

        if   rsi_main and macd_pos:   tc = 10
        elif rsi_main and macd_flat:  tc = 6
        elif rsi_edge or macd_near:   tc = 3

        if r > 70 or r < 40:
            tc = 0

    # ── 6. Bonus (+5 當 CB >= 12 且 PQ >= 10) ───────────────────
    bonus = 5 if (cb >= 12 and pq >= 10) else 0

    new_score = min(40 + cb + pq + vs + rs + tc + bonus, 100)
    breakdown = (
        f'基底+40 · CB+{cb} · PullbackQ+{pq} · '
        f'VS+{vs} · RS+{rs} · TC+{tc} · Bonus+{bonus}'
    )
    return int(new_score), breakdown, cb, pq, vs, rs, tc, bonus


def calc_indicators_and_score(ticker, df, spy_ret, qqq_ret):
    try:
        if df is None or len(df) < 60:
            return None

        df = df.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [str(c).strip() for c in df.columns]

        close  = df['Close']
        volume = df['Volume']

        latest_close = float(close.iloc[-1])
        latest_vol   = float(volume.iloc[-1])

        ema20 = float(_ta.trend.EMAIndicator(close, window=20).ema_indicator().iloc[-1])
        sma50 = float(_ta.trend.SMAIndicator(close, window=50).sma_indicator().iloc[-1])
        rsi14 = float(_ta.momentum.RSIIndicator(close, window=14).rsi().iloc[-1])

        sma200_series = _ta.trend.SMAIndicator(close, window=200).sma_indicator()
        sma200_vals   = sma200_series.dropna()
        sma200 = float(sma200_series.iloc[-1]) if len(sma200_vals) >= 10 else float('nan')  # noqa

        _macd_obj = _ta.trend.MACD(close, window_fast=12, window_slow=26, window_sign=9)
        macd_hist = float(_macd_obj.macd_diff().iloc[-1])

        avg_vol_20 = float(volume.iloc[-20:].mean()) if len(volume) >= 20 else float('nan')
        vol_ratio  = latest_vol / avg_vol_20 if avg_vol_20 > 0 else float('nan')

        ret_1m = float('nan')
        if len(close) >= 22:
            ret_1m = (close.iloc[-1] - close.iloc[-22]) / close.iloc[-22]

        ret_1d = float('nan')
        if len(close) >= 2:
            ret_1d = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]

        high_20 = float(df['High'].iloc[-20:].max()) if len(df) >= 20 else float('nan')

        market_cap   = float('nan')
        company_name = ticker
        try:
            info         = yf.Ticker(ticker).fast_info
            market_cap   = float(getattr(info, 'market_cap', float('nan')) or float('nan'))
            company_name = getattr(yf.Ticker(ticker).info, 'shortName', ticker) or ticker
        except Exception:
            pass

        # ── 硬濾鏡 ──────────────────────────────────────────────
        if np.isnan(market_cap)   or market_cap  < MIN_MARKET_CAP: return None
        if np.isnan(latest_close) or latest_close < MIN_PRICE:     return None
        if np.isnan(avg_vol_20)   or avg_vol_20  < MIN_AVG_VOL:    return None
        if np.isnan(ret_1m)       or ret_1m       < MIN_1M_RETURN:  return None
        if np.isnan(ema20) or np.isnan(sma50):                      return None
        if latest_close <= ema20 or latest_close <= sma50:          return None

        # ── New_Grok_Elite_Score v5.0 ────────────────────────────
        new_score, new_bd, cb_s, pq_s, vs_s, rs_s, tc_s, bonus_s = calc_new_elite_score(
            latest_close, ema20, vol_ratio, high_20, rsi14, macd_hist
        )

        # ── Gap Risk ─────────────────────────────────────────────
        if not np.isnan(ret_1d):
            ret_1d_pct = ret_1d * 100
            if   ret_1d_pct > 6: gap_risk = '高'
            elif ret_1d_pct > 4: gap_risk = '中'
            else:                gap_risk = '低'
        else:
            gap_risk = '低'

        return {
            'Ticker'               : ticker,
            'CN_Name'              : '',
            'New_Grok_Elite_Score' : new_score,
            'Company_Name'         : company_name,
            'Current_Price'        : round(latest_close, 2),
            'Market_Cap_B'         : round(market_cap / 1e9, 1),
            '1M_Return_pct'        : round(ret_1m * 100, 2),
            'RSI'                  : round(rsi14, 1),
            'Volume_Ratio'         : round(vol_ratio, 2),
            'EMA20'                : round(ema20, 2),
            'SMA50'                : round(sma50, 2),
            'MACD_Hist'            : round(macd_hist, 4),
            'High_20'              : round(high_20, 2) if not np.isnan(high_20) else np.nan,
            'Score_Breakdown'      : new_bd,
            'Reason'               : '通過硬濾鏡',
            'Gap_Risk'             : gap_risk,
            'CB_score'             : cb_s,
            'PullbackQ_score'      : pq_s,
            'VS_score'             : vs_s,
            'RS_score'             : rs_s,
            'TC_score'             : tc_s,
            'Bonus_score'          : bonus_s,
            'X_Catalyst_Score'     : '待 Grok 評分',
            'X_Catalyst_Reason'    : '待 Grok 評分',
        }

    except Exception:
        return None


def run_scoring(price_data, spy_ret_1m, qqq_ret_1m):
    print(f'🔢 開始計算 {len(price_data)} 檔股票的 New_Grok_Elite_Score...')
    print('（此步驟需要查詢各股票市值，約需 3-8 分鐘，請耐心等候）\n')

    records = []
    skipped = 0
    total   = len(price_data)

    for i, (ticker, df) in enumerate(price_data.items()):
        result = calc_indicators_and_score(ticker, df, spy_ret_1m, qqq_ret_1m)
        if result:
            records.append(result)
        else:
            skipped += 1

        if (i + 1) % 50 == 0:
            print(f'  進度：{i+1}/{total}，通過篩選：{len(records)}，淘汰：{skipped}')
        time.sleep(0.05)

    print(f'\n✅ 評分完成！通過硬濾鏡：{len(records)} 檔，淘汰：{skipped} 檔')
    return records


# ════════════════════════════════════════════════════════════════
# ⑤ 宏觀快照
# ════════════════════════════════════════════════════════════════
def fetch_macro():
    m = {}
    try:
        hist_bz   = yf.Ticker('BZ=F').history(period='5d')
        brent_px  = float(hist_bz['Close'].dropna().iloc[-1])
        hist_bz1m = yf.Ticker('BZ=F').history(period='1mo')['Close'].dropna()
        brent_chg = (hist_bz1m.iloc[-1] / hist_bz1m.iloc[0] - 1) * 100
        m['brent']      = f'{brent_px:.2f}'
        m['brent_note'] = f"1M {'+' if brent_chg >= 0 else ''}{brent_chg:.1f}%"
    except Exception:
        m['brent'] = 'N/A'
        m['brent_note'] = ''

    def etf_perf(tk):
        try:
            cl   = yf.Ticker(tk).history(period='1y')['Close'].dropna()
            ytdc = cl[cl.index.year == datetime.today().year]
            ytd  = (cl.iloc[-1] / ytdc.iloc[0] - 1) * 100 if len(ytdc) > 0 else float('nan')
            mo1  = (cl.iloc[-1] / cl.iloc[-22] - 1) * 100
            sign = lambda v: '+' if v >= 0 else ''
            return f'{sign(ytd)}{ytd:.1f}%', f'{sign(mo1)}{mo1:.1f}%'
        except Exception:
            return 'N/A', 'N/A'

    m['spy_ytd'], m['spy_1m'] = etf_perf('SPY')
    m['qqq_ytd'], m['qqq_1m'] = etf_perf('QQQ')
    return m


# ════════════════════════════════════════════════════════════════
# ⑥ 中文名稱 / 產業對照表
# ════════════════════════════════════════════════════════════════
CN_NAMES = {
    'AAPL':'蘋果公司','MSFT':'微軟','NVDA':'輝達','AMZN':'亞馬遜',
    'META':'Meta平台','GOOGL':'Alphabet A','GOOG':'Alphabet C',
    'TSLA':'特斯拉','AVGO':'博通','COST':'好市多','NFLX':'網飛',
    'AMD':'超微半導體','ADBE':'Adobe','CSCO':'思科系統',
    'INTC':'英特爾','QCOM':'高通','TXN':'德州儀器','INTU':'Intuit',
    'AMAT':'應用材料','MU':'美光科技','LRCX':'拉姆研究','KLAC':'KLA',
    'PANW':'派拓網路','ISRG':'直覺外科','BKNG':'Booking控股',
    'ADI':'亞德諾半導體','CDNS':'Cadence設計','SNPS':'新思科技',
    'REGN':'再生元製藥','MRNA':'Moderna','BIIB':'渤健','VRTX':'福泰製藥',
    'AMGN':'安進','GILD':'吉利德科學','IDXX':'愛德士',
    'JPM':'摩根大通','BAC':'美國銀行','WFC':'富國銀行',
    'GS':'高盛集團','MS':'摩根士丹利','C':'花旗集團',
    'V':'Visa','MA':'萬事達卡','PYPL':'PayPal','AXP':'美國運通',
    'BLK':'貝萊德','SCHW':'嘉信理財','COF':'Capital One','USB':'美合銀行',
    'JNJ':'嬌生','UNH':'聯合健康','PFE':'輝瑞','ABBV':'艾伯維',
    'MRK':'默克藥廠','LLY':'禮來','TMO':'賽默飛世爾','DHR':'丹納赫',
    'ABT':'雅培','BMY':'必治妥施貴寶','MDT':'美敦力','SYK':'史賽克',
    'ELV':'Elevance健康','CVS':'CVS健康','CI':'Cigna','HUM':'好美公司',
    'XOM':'埃克森美孚','CVX':'雪佛龍','COP':'康菲石油','EOG':'EOG資源',
    'SLB':'斯倫貝謝','OXY':'西方石油','PSX':'菲利普斯66','VLO':'瓦萊羅',
    'NEE':'下一代能源','DUK':'杜克能源','SO':'南方電力','AEP':'美國電力',
    'XEL':'Xcel能源','PCG':'PG&E','ED':'統一愛迪生',
    'WMT':'沃爾瑪','TGT':'塔吉特','HD':'家得寶','LOW':'勞氏',
    'TJX':'TJX公司','ROST':'羅斯百貨','DLTR':'一元樹','DG':'達樂',
    'MCD':'麥當勞','SBUX':'星巴克','YUM':'百勝餐飲','NKE':'耐吉',
    'DIS':'迪士尼','CMCSA':'康卡斯特',
    'PG':'寶潔','KO':'可口可樂','PEP':'百事可樂',
    'PM':'菲利普莫里斯','MO':'奧馳亞','MDLZ':'億滋國際',
    'BA':'波音','RTX':'雷神技術','LMT':'洛克希德馬丁',
    'NOC':'諾思洛普格魯曼','GD':'通用動力',
    'CAT':'卡特彼勒','DE':'迪爾公司','EMR':'艾默生電氣',
    'ETN':'伊頓','ITW':'伊利諾工具','GE':'奇異','HON':'霍尼韋爾',
    'UPS':'聯合包裹','FDX':'聯邦快遞','CSX':'CSX鐵路',
    'UNP':'聯合太平洋','NSC':'諾福克南方',
    'F':'福特汽車','GM':'通用汽車','RIVN':'Rivian',
    'ORCL':'甲骨文','IBM':'IBM','CRM':'Salesforce',
    'NOW':'ServiceNow','SNOW':'雪花計算','PLTR':'帕蘭提爾',
    'UBER':'Uber','ABNB':'Airbnb','DASH':'DoorDash',
    'SPGI':'標普全球','MCO':'穆迪','ICE':'洲際交易所',
    'CME':'芝商所','CBOE':'芝加哥期權所','MSCI':'MSCI',
    'AMT':'美國鐵塔','PLD':'普洛斯','EQIX':'Equinix',
    'CCI':'皇冠城堡','DLR':'數位大樓信託','PSA':'公共儲存',
    'MELI':'美客多','TTD':'交易台','DDOG':'Datadog',
    'TEAM':'Atlassian','ZS':'Zscaler','CRWD':'CrowdStrike',
    'FTNT':'飛塔','NET':'Cloudflare','WDAY':'Workday',
    'OKTA':'Okta','ZM':'Zoom','DOCU':'DocuSign',
    'ON':'安森美','NXPI':'恩智浦','MRVL':'美滿電子',
    'ENPH':'安費諾','FSLR':'第一太陽能','CEG':'星座能源',
    'NEM':'紐蒙特','FCX':'自由港麥克','ALB':'雅保',
    'LIN':'林德','APD':'空氣產品','ECL':'藝康','SHW':'宣偉',
    'DHI':'D.R.霍頓','LEN':'萊納房屋','PHM':'普爾特',
    'ASML':'ASML','TSM':'台積電','MCHP':'微芯科技',
    'MSTR':'MicroStrategy','COIN':'Coinbase','HOOD':'Robinhood',
    'SOFI':'SoFi','NU':'Nu Holdings','AFRM':'Affirm',
    'CPRT':'Copart','ODFL':'老道明','FAST':'法斯特爾',
    'PAYX':'Paychex','ADP':'自動數據',
    'FIS':'Fidelity資訊','FI':'富達國際','BR':'博睿信息',
    'PNC':'PNC金融','TFC':'特魯斯特金融',
    'AIG':'美國國際集團','MET':'大都會人壽','PRU':'保德信金融',
    'AFL':'美國家庭','MMC':'達信集團','AON':'怡安集團','CB':'丘博集團',
    'ACN':'埃森哲','SAP':'思愛普',
    'LULU':'露露樂蒙','DECK':'Deckers戶外',
    'LVS':'拉斯維加斯金沙','WYNN':'永利渡假','MGM':'美高梅',
    'MAR':'萬豪國際','HLT':'希爾頓',
    'T':'美國電話電報','VZ':'Verizon','TMUS':'T-Mobile',
    'SPOT':'Spotify','PINS':'Pinterest','SNAP':'Snapchat','RBLX':'Roblox',
    'LCID':'Lucid汽車','XPEV':'小鵬汽車','NIO':'蔚來汽車','LI':'理想汽車',
    'SQ':'Block(Square)',
    'O':'Realty Income','VICI':'維奇地產',
    'DXCM':'德康醫療','ALGN':'愛齊科技',
    'CMG':'奇波雷墨西哥','WING':'Wingstop',
    'DVN':'Devon能源','MRO':'Marathon石油','HAL':'哈里伯頓',
    'CLF':'Cleveland-Cliffs','NUE':'紐柯鋼鐵',
    'TDG':'TransDigm','HEI':'Heico',
    'WM':'廢物管理','RSG':'共和服務',
    'URI':'United租賃',
    'HUBB':'Hubbell','GNRC':'Generac',
    'FANG':'鑽石背能源',
}

SECTOR_ZH = {
    'Technology'            : '科技',
    'Healthcare'            : '醫療',
    'Financial Services'    : '金融',
    'Consumer Cyclical'     : '週期消費',
    'Consumer Defensive'    : '民生消費',
    'Industrials'           : '工業',
    'Energy'                : '能源',
    'Basic Materials'       : '原物料',
    'Real Estate'           : '房地產',
    'Utilities'             : '公用事業',
    'Communication Services': '通訊媒體',
}

_sector_cache = {}


def get_cn_name(ticker, en_name=''):
    return CN_NAMES.get(str(ticker).upper(), en_name or ticker)


def get_sector(ticker):
    tk = str(ticker).upper()
    if tk in _sector_cache:
        return _sector_cache[tk]
    try:
        info      = yf.Ticker(tk).info
        sector_en = info.get('sector', '') or ''
        result    = SECTOR_ZH.get(sector_en, sector_en or '—')
    except Exception:
        result = '—'
    _sector_cache[tk] = result
    return result


# ════════════════════════════════════════════════════════════════
# ⑦ Plotly K 線圖
# ════════════════════════════════════════════════════════════════
def create_kline_plotly(ticker, df_raw, rank, score, cn_name, signal_text=''):
    try:
        df    = df_raw.copy().dropna()
        n     = min(80, len(df))
        d     = df.iloc[-n:].copy()
        cl    = df['Close']
        ema20 = _ta.trend.EMAIndicator(cl, window=20).ema_indicator().iloc[-n:]
        sma50 = _ta.trend.SMAIndicator(cl, window=50).sma_indicator().iloc[-n:]
        rsi14 = _ta.momentum.RSIIndicator(cl, window=14).rsi().iloc[-n:]
        _macd_kline = _ta.trend.MACD(cl, window_fast=12, window_slow=26, window_sign=9)
        m_l   = _macd_kline.macd().iloc[-n:]
        m_s   = _macd_kline.macd_signal().iloc[-n:]
        m_h   = _macd_kline.macd_diff().iloc[-n:]
        dates = d.index.strftime('%Y-%m-%d').tolist()
        v_col = ['#30D158' if float(c) >= float(o) else '#FF453A'
                 for c, o in zip(d['Close'], d['Open'])]
        h_col = ['#30D158' if float(v) >= 0 else '#FF453A' for v in m_h]

        fig = make_subplots(
            rows=4, cols=1, shared_xaxes=True,
            row_heights=[0.50, 0.15, 0.175, 0.175],
            vertical_spacing=0.018
        )
        fig.add_trace(go.Candlestick(
            x=dates, open=d['Open'].tolist(), high=d['High'].tolist(),
            low=d['Low'].tolist(), close=d['Close'].tolist(),
            increasing_line_color='#30D158', decreasing_line_color='#FF453A',
            increasing_fillcolor='#30D158', decreasing_fillcolor='#FF453A',
            name='K線', showlegend=False
        ), row=1, col=1)
        fig.add_trace(go.Scatter(x=dates, y=ema20.tolist(), name='EMA20',
            line=dict(color='#FF9F0A', width=1.8)), row=1, col=1)
        fig.add_trace(go.Scatter(x=dates, y=sma50.tolist(), name='SMA50',
            line=dict(color='#0A84FF', width=1.8)), row=1, col=1)
        fig.add_trace(go.Bar(x=dates, y=d['Volume'].tolist(),
            marker_color=v_col, name='Volume', showlegend=False), row=2, col=1)
        fig.add_trace(go.Scatter(x=dates, y=rsi14.tolist(), name='RSI(14)',
            line=dict(color='#FFD60A', width=1.5)), row=3, col=1)
        for lvl, clr in [(70, '#FF453A'), (50, '#48484a'), (30, '#30D158')]:
            fig.add_hline(y=lvl, line_dash='dot', line_color=clr, line_width=1, row=3, col=1)
        fig.add_trace(go.Bar(x=dates, y=m_h.tolist(), marker_color=h_col,
            name='Histogram', showlegend=False), row=4, col=1)
        fig.add_trace(go.Scatter(x=dates, y=m_l.tolist(), name='MACD',
            line=dict(color='#0A84FF', width=1.5)), row=4, col=1)
        fig.add_trace(go.Scatter(x=dates, y=m_s.tolist(), name='Signal',
            line=dict(color='#FF453A', width=1.5)), row=4, col=1)

        fig.update_layout(
            title=dict(
                text=f'#{rank} {ticker} · {cn_name} · Score {score}',
                font=dict(size=15, color='#E5E5EA'), x=0.01
            ),
            paper_bgcolor='#161b22', plot_bgcolor='#0d1117',
            font=dict(color='#8b949e', family='Noto Sans TC, sans-serif', size=11),
            height=580, margin=dict(l=55, r=25, t=52, b=15),
            xaxis_rangeslider_visible=False,
            legend=dict(orientation='h', yanchor='bottom', y=1.01,
                        xanchor='right', x=1, font=dict(size=10),
                        bgcolor='rgba(0,0,0,0)')
        )
        for i in range(1, 5):
            fig.update_xaxes(gridcolor='#21262d', row=i, col=1, showgrid=True)
            fig.update_yaxes(gridcolor='#21262d', row=i, col=1, showgrid=True)

        return pio.to_html(fig, include_plotlyjs='cdn', full_html=False,
                           config={'displayModeBar': False, 'responsive': True})
    except Exception as e:
        return f'<p style="color:#636366;padding:20px">⚠️ {ticker} 圖表產生失敗：{e}</p>'


# ════════════════════════════════════════════════════════════════
# ⑧ HTML 報告輔助函式
# ════════════════════════════════════════════════════════════════
def sc_color(s):
    if s >= 80: return '#30D158'
    if s >= 65: return '#D4832A'
    if s >= 55: return '#FF6B35'
    return '#FF453A'


def risk_label(score):
    if score >= 80:   return '低',   '#30D158'
    elif score >= 65: return '中',   '#D4832A'
    elif score >= 55: return '中高', '#FF6B35'
    else:             return '高',   '#FF453A'


def score_bar(s):
    c   = sc_color(s)
    pct = min(s, 100)
    return (
        f'<div style="display:flex;align-items:center;gap:10px;">'
        f'<div style="width:60%;height:7px;background:#21262d;border-radius:4px;'
        f'overflow:hidden;flex-shrink:0;">'
        f'<div style="width:{pct:.0f}%;height:100%;background:{c};border-radius:4px;"></div>'
        f'</div>'
        f'<span style="color:{c};font-weight:800;font-size:1.08em;min-width:36px;">{s}</span>'
        f'</div>'
    )


def rsi_fmt(v):
    if v >= 70:   c, icon = '#FF453A', '⚠'
    elif v >= 55: c, icon = '#30D158', ''
    elif v >= 40: c, icon = '#FF9F0A', ''
    else:         c, icon = '#636366', ''
    lbl = f'{icon} {v:.0f}' if icon else f'{v:.0f}'
    return f'<span style="color:{c};font-weight:700">{lbl}</span>'


def ret_fmt(v):
    c = '#30D158' if v >= 0 else '#FF453A'
    return f'<span style="color:{c};font-weight:600">{("+" if v >= 0 else "")}{v:.2f}%</span>'


def vr_fmt(v):
    if v >= 2.0:   c = '#30D158'
    elif v >= 1.5: c = '#74C27A'
    elif v >= 1.2: c = '#C07A2A'
    else:          c = '#636366'
    return f'<span style="color:{c};font-weight:600">{v:.2f}x</span>'


def rank_icon(r):
    return {1: '🥇', 2: '🥈', 3: '🥉'}.get(r, f'<span style="color:#636366">#{r}</span>')


# ════════════════════════════════════════════════════════════════
# ⑨ 生成 HTML 報告
# ════════════════════════════════════════════════════════════════
def generate_html_report(top10_df, all_df, pdata, today_str, macro=None):
    if macro is None:
        macro = {}
    n_scan   = len(pdata)
    n_pass   = len(all_df)
    t1       = top10_df.iloc[0]
    t1_tick  = t1['Ticker']
    t1_cn    = get_cn_name(t1_tick, t1.get('Company_Name', ''))
    t1_score = int(t1.get('New_Grok_Elite_Score', 40) or 40)
    date_fmt = datetime.strptime(today_str, '%Y%m%d').strftime('%Y/%m/%d')

    rows = ''
    for _, r in all_df.iterrows():
        rk    = int(r.get('Rank', 0))
        tk    = str(r.get('Ticker', ''))
        cn    = get_cn_name(tk, r.get('Company_Name', ''))
        sc    = int(r.get('New_Grok_Elite_Score', 40) or 40)
        pr    = float(r.get('Current_Price', 0) or 0)
        mc    = float(r.get('Market_Cap_B', 0) or 0)
        rt    = float(r.get('1M_Return_pct', 0) or 0)
        rs    = float(r.get('RSI', 0) or 0)
        vr    = float(r.get('Volume_Ratio', 0) or 0)
        bd    = str(r.get('Score_Breakdown', '')).replace('|', '·')
        re_   = str(r.get('Reason', ''))
        gap_r = str(r.get('Gap_Risk', '低'))
        sec   = get_sector(tk)
        rlv, rc = risk_label(sc)
        bg    = 'background:#1c2128;' if rk % 2 == 0 else ''
        bl    = {1: '#FFD60A', 2: '#C0C0C0', 3: '#CD7F32'}.get(rk, '')
        bl_s  = f'border-left:3px solid {bl};' if bl else ''
        risk_badge = (
            f'<span style="display:inline-block;padding:3px 10px;border-radius:20px;'
            f'font-size:.75em;font-weight:800;letter-spacing:.04em;'
            f'background:{rc}22;color:{rc};border:1px solid {rc}55">{rlv}</span>'
        )
        gap_c = '#FF453A' if gap_r == '高' else ('#FF9F0A' if gap_r == '中' else '#30D158')
        rows += (
            f'<tr style="{bg}{bl_s}">'
            f'<td style="text-align:center;padding:10px 6px;font-size:1.1em">{rank_icon(rk)}</td>'
            f'<td style="padding:10px 8px"><span style="font-weight:800;color:#E5E5EA;'
            f'font-size:1.02em;letter-spacing:.04em">{tk}</span></td>'
            f'<td style="padding:8px 8px;vertical-align:middle">'
            f'<div style="font-size:.97em;color:#E5E5EA;font-weight:500;word-break:break-word">{cn}</div>'
            f'<div style="margin-top:4px"><span style="display:inline-block;padding:1px 7px;'
            f'border-radius:4px;font-size:.70em;font-weight:600;color:#8b949e;'
            f'background:#21262d">{sec}</span></div></td>'
            f'<td style="padding:8px 10px 8px 8px;vertical-align:middle;min-width:155px">'
            f'<div style="display:flex;align-items:center;gap:8px">'
            f'{risk_badge}<div style="flex:1">{score_bar(sc)}</div></div></td>'
            f'<td style="padding:10px 8px;text-align:right;color:#E5E5EA;'
            f'font-family:monospace;font-size:1.02em">${pr:.2f}</td>'
            f'<td style="padding:10px 8px;text-align:right;color:#8b949e;font-size:.86em">'
            f'${mc:.1f}B</td>'
            f'<td style="padding:10px 8px;text-align:center">{ret_fmt(rt)}</td>'
            f'<td style="padding:10px 8px;text-align:center">{rsi_fmt(rs)}</td>'
            f'<td style="padding:10px 8px;text-align:center">{vr_fmt(vr)}</td>'
            f'<td style="padding:10px 8px;color:#58a6ff;font-size:.72em;min-width:135px;'
            f'max-width:135px;word-break:break-word;line-height:1.55;vertical-align:top">{bd}</td>'
            f'<td style="padding:10px 8px;color:#c9d1d9;font-size:.88em;min-width:190px;'
            f'max-width:230px;word-break:break-word;line-height:1.6;vertical-align:top">{re_}</td>'
            f'<td style="padding:6px 4px;text-align:center;width:36px;white-space:nowrap;'
            f'vertical-align:middle"><span style="color:{gap_c};font-weight:800;font-size:.85em">'
            f'{gap_r}</span></td></tr>'
        )

    charts = ''
    print('  生成 K 線圖中...')
    for idx, (_, r) in enumerate(top10_df.head(10).iterrows()):
        tk       = str(r.get('Ticker', ''))
        rk       = int(r.get('Rank', idx + 1))
        sc       = int(r.get('New_Grok_Elite_Score', 40) or 40)
        cn       = get_cn_name(tk, r.get('Company_Name', ''))
        bd       = str(r.get('Score_Breakdown', ''))
        pr       = float(r.get('Current_Price', 0) or 0)
        rt       = float(r.get('1M_Return_pct', 0) or 0)
        ema20_v  = float(r.get('EMA20', 0) or 0)
        sma50_v  = float(r.get('SMA50', 0) or 0)
        rsi_v    = float(r.get('RSI', 0) or 0)
        vr_v     = float(r.get('Volume_Ratio', 0) or 0)
        above_ema    = pr >= ema20_v if ema20_v > 0 else False
        above_sma50  = pr >= sma50_v if sma50_v > 0 else False
        rsi_ok       = 40 <= rsi_v <= 72
        vr_ok        = vr_v >= 1.5
        ret_ok       = rt >= 8.0
        rlv, rc      = risk_label(sc)
        score_col    = sc_color(sc)

        def sig_vr(vr_val, txt):
            if vr_val >= 2.0:   col, icon = '#30D158', '✅'
            elif vr_val >= 1.5: col, icon = '#74C27A', '✅'
            elif vr_val >= 1.2: col, icon = '#C07A2A', '⚠️'
            else:               col, icon = '#636366', '⚠️'
            return f'<span style="color:{col};white-space:nowrap">{icon}&nbsp;{txt}</span>'

        def sig_rsi(rsi_val, txt):
            if rsi_val >= 70:   col, icon = '#FF453A', '⚠️'
            elif rsi_val >= 55: col, icon = '#30D158', '✅'
            elif rsi_val >= 40: col, icon = '#C07A2A', '⚠️'
            else:               col, icon = '#636366', '⚠️'
            return f'<span style="color:{col};white-space:nowrap">{icon}&nbsp;{txt}</span>'

        def sig(ok, txt):
            col  = '#30D158' if ok else '#636366'
            icon = '✅' if ok else '⚠️'
            return f'<span style="color:{col};white-space:nowrap">{icon}&nbsp;{txt}</span>'

        strip = (
            f'<div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px 18px;'
            f'padding:11px 20px;background:#161b22;border-bottom:1px solid #21262d;'
            f'font-size:.82em;">'
            f'<span style="font-size:1.15em">{rank_icon(rk)}</span>'
            f'<span style="background:{rc}22;color:{rc};border:1px solid {rc}55;'
            f'padding:2px 10px;border-radius:20px;font-size:.8em;font-weight:800">'
            f'風險：{rlv}</span>'
            f'<span style="font-weight:900;color:#E5E5EA;font-size:1.05em;'
            f'letter-spacing:.04em">{tk}</span>'
            f'<span style="color:#8b949e;font-size:.9em">{cn}</span>'
            f'<span style="color:{score_col};font-weight:800">{sc}分</span>'
            f'<span style="color:#30363d">│</span>'
            f'{sig(above_ema, f"EMA20 ${ema20_v:.2f}")}'
            f'{sig(above_sma50, f"SMA50 ${sma50_v:.2f}")}'
            f'{sig_rsi(rsi_v, f"RSI {rsi_v:.0f}")}'
            f'{sig_vr(vr_v, f"量比 {vr_v:.2f}x")}'
            f'{sig(ret_ok, f"1M +{rt:.1f}%")}'
            f'<span style="color:#30363d">│</span>'
            f'<span style="color:#E5E5EA;font-family:monospace;font-weight:700">'
            f'${pr:.2f} USD</span>'
            f'</div>'
        )

        ch = (
            create_kline_plotly(tk, pdata[tk], rk, sc, cn, bd)
            if tk in pdata
            else f'<p style="color:#636366;padding:20px">⚠️ {tk} 無 K 線資料</p>'
        )
        print(f'    #{rk} {tk} ✓')
        charts += (
            f'<div style="background:#0d1117;border:1px solid #21262d;border-radius:14px;'
            f'margin-bottom:24px;overflow:hidden;">'
            f'{strip}<div>{ch}</div></div>'
        )

    legend_html = '''
<div class="sec" style="padding-top:20px">
  <div style="background:#161b22;border:1px solid #21262d;border-radius:10px;padding:20px 24px;margin-top:24px">
    <div style="font-weight:800;color:#E5E5EA;margin-bottom:13px;font-size:1.02em">📐 細化 Grok Elite Score v5.0 評分說明</div>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:9px;font-size:.81em;color:#8b949e">
      <div>✅ 通過硬濾鏡 → <span style="color:#E5E5EA">基底 40 分</span></div>
      <div>📦 Consolidation Breakout → <span style="color:#FFD60A">最高 +18 分</span></div>
      <div>🎯 Pullback Quality → <span style="color:#FF9F0A">最高 +15 分</span></div>
      <div>📈 Volume Surge → <span style="color:#30D158">最高 +12 分</span></div>
      <div>💪 Relative Strength → <span style="color:#30D158">+8 分</span></div>
      <div>⚙️ Technical Confirmation → <span style="color:#30D158">最高 +10 分</span></div>
      <div>🎁 雙重共振 Bonus → <span style="color:#FFD60A">+5 分</span></div>
      <div style="grid-column:1/-1;margin-top:8px;padding-top:8px;border-top:1px solid #21262d">
        <span style="background:#30D15822;color:#30D158;border:1px solid #30D15855;padding:2px 10px;border-radius:20px;font-size:.85em;font-weight:700;margin-right:6px">80+ 低風險</span>
        <span style="background:#D4832A22;color:#D4832A;border:1px solid #D4832A55;padding:2px 10px;border-radius:20px;font-size:.85em;font-weight:700;margin-right:6px">65-79 中風險</span>
        <span style="background:#FF6B3522;color:#FF6B35;border:1px solid #FF6B3555;padding:2px 10px;border-radius:20px;font-size:.85em;font-weight:700;margin-right:6px">55-64 中高風險</span>
        <span style="background:#FF453A22;color:#FF453A;border:1px solid #FF453A55;padding:2px 10px;border-radius:20px;font-size:.85em;font-weight:700">&lt;55 高風險</span>
      </div>
    </div>
    <div style="margin-top:14px;padding-top:12px;border-top:1px solid #21262d;font-size:.74em;color:#484f58">
      ⚠️ 本報告為純技術面系統篩選，不構成投資建議。波段交易具有虧損風險，請嚴守停損（-7%～-10%）。
    </div>
  </div>
</div>'''

    m = macro
    spy_ytd_c = '#30D158' if not str(m.get('spy_ytd', '-')).startswith('-') else '#FF453A'
    qqq_ytd_c = '#30D158' if not str(m.get('qqq_ytd', '-')).startswith('-') else '#FF453A'
    _brent      = m.get('brent', 'N/A')
    _brent_note = m.get('brent_note', '')
    _spy_ytd    = m.get('spy_ytd', 'N/A')
    _spy_1m     = m.get('spy_1m', 'N/A')
    _qqq_ytd    = m.get('qqq_ytd', 'N/A')
    _qqq_1m     = m.get('qqq_1m', 'N/A')

    macrobar_html = (
        f'<div class="macrobar">'
        f'<div class="mbi"><span class="mbl">🛢 Brent 原油</span>'
        f'<span class="mbv" style="color:#FF9F0A">${_brent} USD</span>'
        f'<span class="mbn">{_brent_note}</span></div>'
        f'<div class="mbi"><span class="mbl">📈 SPY</span>'
        f'<span class="mbv" style="color:{spy_ytd_c}">YTD {_spy_ytd}</span>'
        f'<span class="mbn">1M {_spy_1m}</span></div>'
        f'<div class="mbi"><span class="mbl">📊 QQQ</span>'
        f'<span class="mbv" style="color:{qqq_ytd_c}">YTD {_qqq_ytd}</span>'
        f'<span class="mbn">1M {_qqq_1m}</span></div>'
        f'</div>'
    )

    return f'''<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Grok Elite Swing · {date_fmt}</title>
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@300;400;500;700;900&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html{{font-size:14px;scroll-behavior:smooth}}
body{{font-family:'Noto Sans TC',system-ui,sans-serif;background:#0d1117;color:#c9d1d9;padding-bottom:80px}}
.header{{background:linear-gradient(160deg,#0d1117 0%,#161b22 55%,#0d1117 100%);border-bottom:1px solid #21262d;padding:0}}
.hi{{max-width:1440px;margin:0 auto;padding:22px 36px 0}}
.brand{{font-size:.75em;color:#58a6ff;font-weight:600;letter-spacing:.18em;text-transform:uppercase;margin-bottom:5px}}
.htitle{{font-size:1.8em;font-weight:900;color:#E5E5EA;letter-spacing:-.02em;margin-bottom:14px}}
.htitle span{{color:#FFD60A}}
.hmeta{{display:flex;flex-wrap:wrap;gap:6px;font-size:.76em;color:#58a6ff;margin-bottom:22px}}
.hmeta span{{background:#21262d;padding:3px 10px;border-radius:4px;border:1px solid #30363d}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px;max-width:1440px;margin:0 auto;padding:0 36px 22px}}
.card{{background:#161b22;border:1px solid #21262d;border-radius:11px;padding:17px 20px;transition:border-color .2s}}
.card:hover{{border-color:#30363d}}
.card.gold{{border-color:#FFD60A55}}
.clabel{{font-size:.7em;color:#8b949e;font-weight:600;text-transform:uppercase;letter-spacing:.09em;margin-bottom:7px}}
.cval{{font-size:1.65em;font-weight:800;color:#E5E5EA;line-height:1;margin-bottom:3px}}
.card.gold .cval{{color:#FFD60A}}
.csub{{font-size:.7em;color:#8b949e}}
.macrobar{{max-width:1440px;margin:0 auto;padding:12px 36px;display:grid;grid-template-columns:repeat(3,1fr);gap:10px;background:#0d1117;border-bottom:1px solid #21262d}}
.mbi{{background:#161b22;border:1px solid #21262d;border-radius:9px;padding:10px 16px;display:flex;flex-direction:column;gap:3px}}
.mbl{{font-size:.67em;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#8b949e}}
.mbv{{font-size:1.05em;font-weight:800;color:#E5E5EA;line-height:1.2}}
.mbn{{font-size:.72em;color:#636366;line-height:1.4}}
.sec{{max-width:1440px;margin:0 auto;padding:32px 36px 0}}
.sechead{{display:flex;align-items:flex-start;gap:14px;margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid #21262d}}
.sectl{{font-size:1.05em;font-weight:800;color:#E5E5EA}}
.secbadge{{background:#21262d;color:#8b949e;padding:2px 10px;border-radius:20px;font-size:.73em;vertical-align:middle;margin-left:6px}}
.secdesc{{font-size:.77em;color:#8b949e;margin-top:5px}}
.leg{{display:flex;gap:14px;flex-wrap:wrap;font-size:.74em;color:#8b949e;margin-top:8px}}
.ldot{{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:4px;vertical-align:middle}}
.twrap{{overflow-x:auto;border:1px solid #21262d;border-radius:12px;background:#161b22}}
table{{width:100%;border-collapse:collapse;font-size:.87em}}
thead tr{{background:#1c2128;border-bottom:2px solid #30363d}}
th{{padding:11px 8px;text-align:left;font-weight:700;color:#8b949e;font-size:.79em;text-transform:uppercase;letter-spacing:.07em;white-space:nowrap}}
td{{border-bottom:1px solid #1c2128}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:rgba(56,139,253,.04)}}
.footer{{text-align:center;padding:44px 36px 20px;color:#484f58;font-size:.73em;border-top:1px solid #21262d;margin-top:48px;max-width:1440px;margin-left:auto;margin-right:auto}}
</style>
</head>
<body>
<div class="header">
  <div class="hi">
    <div class="brand">Grok Elite Swing &nbsp;·&nbsp; 機密報告</div>
    <h1 class="htitle">Grok Elite Swing v5.0 — <span>{date_fmt}</span> 收盤後分析</h1>
    <div class="hmeta">
      <span>📡 S&amp;P 500 + Nasdaq 100</span>
      <span>⏱ 7-14 天波段策略</span>
      <span>📊 yfinance · 免費資料源</span>
      <span>⚠️ 僅供參考，不構成投資建議</span>
    </div>
  </div>
  <div class="stats">
    <div class="card"><div class="clabel">掃描標的</div><div class="cval">{n_scan}</div><div class="csub">S&amp;P500 + Nasdaq100</div></div>
    <div class="card"><div class="clabel">通過篩選</div><div class="cval" style="color:#30D158">{n_pass}</div><div class="csub">硬濾鏡 + 趨勢確認</div></div>
    <div class="card gold"><div class="clabel">今日 Top 1</div><div class="cval">{t1_tick}</div><div class="csub">{t1_cn} · 細化評分 {t1_score}</div></div>
    <div class="card"><div class="clabel">報告日期</div><div class="cval" style="font-size:1.15em">{date_fmt}</div><div class="csub">收盤後分析</div></div>
    <div class="card"><div class="clabel">持倉週期</div><div class="cval" style="font-size:1.2em">7-14天</div><div class="csub">波段策略</div></div>
  </div>
</div>
{macrobar_html}
<div class="sec">
  <div class="sechead">
    <div>
      <div class="sectl">🔥 完整排行榜 <span class="secbadge">{n_pass} 檔</span></div>
      <div class="secdesc">硬濾鏡：市值 &gt;$10B · 股價 &gt;$10 · 日均量 &gt;100萬 · 1M漲幅 &gt;8% · 站上 EMA20 &amp; SMA50</div>
      <div class="leg">
        <span><span class="ldot" style="background:#30D158"></span>80+ 低風險</span>
        <span><span class="ldot" style="background:#FF9F0A"></span>65-79 中風險</span>
        <span><span class="ldot" style="background:#FF6B35"></span>55-64 中高風險</span>
        <span><span class="ldot" style="background:#FF453A"></span>&lt;55 高風險</span>
      </div>
    </div>
  </div>
  <div class="twrap">
    <table>
      <thead>
        <tr>
          <th style="text-align:center;width:44px">排行</th>
          <th style="width:70px">代碼</th>
          <th style="min-width:130px">名稱／產業</th>
          <th style="min-width:155px">細化評分／風險</th>
          <th style="text-align:right;white-space:nowrap">收盤價</th>
          <th style="text-align:right;white-space:nowrap">市值</th>
          <th style="text-align:center;white-space:nowrap">1M漲幅</th>
          <th style="text-align:center;width:52px">RSI</th>
          <th style="text-align:center;width:58px">量比</th>
          <th style="min-width:135px">細化分項</th>
          <th style="min-width:190px">關鍵理由</th>
          <th style="width:36px;text-align:center;white-space:nowrap">Gap</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>
<div class="sec" style="padding-top:36px">
  <div class="sechead">
    <div>
      <div class="sectl">📊 Top 10 K 線圖 <span class="secbadge">EMA20 橙 · SMA50 藍 · RSI · MACD</span></div>
      <div class="secdesc">最近 80 個交易日 · 深色模式 · Plotly 互動（可縮放 / 拖曳）</div>
    </div>
  </div>
  {charts}
</div>
{legend_html}
<div class="footer">Grok Elite Swing Model v5.0 &nbsp;·&nbsp; {date_fmt} &nbsp;·&nbsp; 資料來源：yfinance / Wikipedia &nbsp;·&nbsp; 僅供內部參考，不構成投資建議</div>
</body></html>'''


# ════════════════════════════════════════════════════════════════
# ⑩ 匯出 CSV + HTML
# ════════════════════════════════════════════════════════════════
def export_results(records, price_data, macro):
    if not records:
        print('⚠️ 無資料可匯出')
        return

    results_df = pd.DataFrame(records)

    # 補中文名
    results_df['CN_Name'] = results_df.apply(
        lambda r: get_cn_name(r['Ticker'], r.get('Company_Name', '')), axis=1
    )

    # 風險等級 + 排序
    results_df['風險等級'] = results_df['New_Grok_Elite_Score'].apply(
        lambda s: risk_label(s)[0]
    )
    results_df = results_df.sort_values(
        by=['New_Grok_Elite_Score', 'PullbackQ_score', '1M_Return_pct', 'Volume_Ratio'],
        ascending=[False, False, False, False]
    ).reset_index(drop=True)
    results_df['Final_Rank'] = results_df.index + 1
    results_df['Rank']       = results_df['Final_Rank']

    top10 = results_df.head(10).copy()

    # 控制台 Top 10 預覽
    print('\n' + '=' * 90)
    print('📋 Top 10 細化評分預覽：')
    print('=' * 90)
    preview_cols = [c for c in [
        'Final_Rank', 'Ticker', 'CN_Name', 'New_Grok_Elite_Score',
        'CB_score', 'PullbackQ_score', 'VS_score', 'RS_score', 'TC_score', 'Bonus_score',
        '風險等級', 'Current_Price', '1M_Return_pct', 'RSI', 'Score_Breakdown'
    ] if c in top10.columns]
    print(top10[preview_cols].to_string(index=False))

    export_cols = [
        'Final_Rank', 'Rank', 'Ticker', 'CN_Name', 'New_Grok_Elite_Score',
        '風險等級', 'Company_Name', 'Current_Price', 'Market_Cap_B',
        '1M_Return_pct', 'RSI', 'Volume_Ratio', 'EMA20', 'SMA50', 'MACD_Hist',
        'High_20', 'Gap_Risk', 'Score_Breakdown', 'Reason',
        'CB_score', 'PullbackQ_score', 'VS_score', 'RS_score', 'TC_score', 'Bonus_score',
        'X_Catalyst_Score', 'X_Catalyst_Reason',
    ]
    safe_cols = [c for c in export_cols if c in results_df.columns]

    # 檔案 1：Top 10 CSV
    f1 = f'Grok_Elite_Swing_Top10_{TODAY}.csv'
    top10[safe_cols].to_csv(f1, index=False, encoding='utf-8-sig')
    print(f'\n✅ 檔案①：{f1}')

    # 檔案 2：全部篩選股 CSV
    f2 = f'Grok_Elite_Swing_ALL_{TODAY}.csv'
    results_df[safe_cols].to_csv(f2, index=False, encoding='utf-8-sig')
    print(f'✅ 檔案②：{f2}（共 {len(results_df)} 檔）')

    # 檔案 3：HTML 精美報告
    f3 = f'Grok_Elite_Swing_Report_{TODAY}.html'
    print(f'\n⏳ 正在生成 HTML 報告（含 Top 10 K 線圖）...')
    html_content = generate_html_report(top10, results_df, price_data, TODAY, macro)
    with open(f3, 'w', encoding='utf-8') as fh:
        fh.write(html_content)
    print(f'✅ 檔案③：{f3}')

    print(f'\n🎉 完成！請在當前目錄取得以上三個檔案。')

    # ── （選用）Google Drive 儲存 ─────────────────────────────
    # 取消以下註解，並在 Colab 環境中執行即可自動儲存至 Drive
    # from google.colab import drive
    # import shutil
    # drive.mount('/content/drive')
    # GDRIVE_FOLDER = '/content/drive/MyDrive/GrokEliteSwing'
    # os.makedirs(GDRIVE_FOLDER, exist_ok=True)
    # for fname in [f1, f2, f3]:
    #     shutil.copy(fname, os.path.join(GDRIVE_FOLDER, fname))
    #     print(f'✅ 已儲存至 Google Drive：{fname}')

    return results_df, top10, f1, f2, f3


# ════════════════════════════════════════════════════════════════
# ⑪ 統計摘要
# ════════════════════════════════════════════════════════════════
def print_summary(all_tickers, price_data, records, spy_ret_1m, qqq_ret_1m):
    if not records:
        return
    print('=' * 60)
    print(f'📊 今日執行摘要 — {datetime.today().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 60)
    print(f'  股票池：{len(all_tickers)} 檔')
    print(f'  資料下載成功：{len(price_data)} 檔')
    print(f'  通過硬濾鏡：{len(records)} 檔')
    print(f'  篩選率：{len(records) / len(price_data) * 100:.1f}%')
    print(f'  SPY 近 1M 報酬：{spy_ret_1m:.2%}')
    print(f'  QQQ 近 1M 報酬：{qqq_ret_1m:.2%}')
    print()

    all_scores = [r['New_Grok_Elite_Score'] for r in records]
    print('  評分分佈：')
    print(f'    平均分：{np.mean(all_scores):.1f}')
    print(f'    中位數：{np.median(all_scores):.1f}')
    print(f'    最高分：{max(all_scores)}')
    print(f'    80+ 分：{sum(s >= 80 for s in all_scores)} 檔')
    print(f'    70+ 分：{sum(s >= 70 for s in all_scores)} 檔')
    print(f'    60+ 分：{sum(s >= 60 for s in all_scores)} 檔')
    print()
    print('─' * 60)
    print('✅ 本次篩選完成！建議將 CSV 上傳至 Grok 進行二次確認。')
    print('─' * 60)


# === 通知發送 ===
import os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

TODAY_STR = datetime.today().strftime('%Y/%m/%d')

def build_telegram_message(top10_df, today_str, macro):
    lines = [
        f"📊 *Grok Elite Swing — {today_str}*",
        f"SPY 1M {macro.get('spy_1m','N/A')} | QQQ 1M {macro.get('qqq_1m','N/A')}",
        '─' * 28,
    ]
    for _, r in top10_df.iterrows():
        rk  = int(r.get('Final_Rank', r.get('Rank', 0)))
        tk  = r['Ticker']
        cn  = r.get('CN_Name', '')
        sc  = r['New_Grok_Elite_Score']
        risk= r.get('風險等級', '')
        ret = r.get('1M_Return_pct', 0)
        vr  = r.get('Volume_Ratio', 0)
        gap = r.get('Gap_Risk', '低')
        bd  = r.get('Score_Breakdown', '')
        gi  = '🔴' if gap=='高' else ('🟡' if gap=='中' else '🟢')
        lines.append(
            f"#{rk} *{tk}* {cn}  `{sc:.0f}分` {gi}\n"
            f"   1M:{ret:+.1f}%  量比:{vr:.2f}x  風險:{risk}\n"
            f"   {bd}"
        )
    lines.append('─' * 28)
    gh_url = os.environ.get('PAGES_URL', '')
    if gh_url:
        lines.append(f"🔗 [完整報告]({gh_url})")
    lines.append("_GitHub Actions 自動發送_")
    return '\n'.join(lines)


def send_telegram(message):
    token   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
    if not token or not chat_id:
        print('⚠️  Telegram 未設定，跳過')
        return
    import urllib.request, urllib.parse
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        'chat_id': chat_id, 'text': message,
        'parse_mode': 'Markdown', 'disable_web_page_preview': 'false'
    }).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15)
        print('✅ Telegram 發送成功')
    except Exception as e:
        print(f'❌ Telegram 失敗：{e}')


def build_email_html(top10_df, today_str, macro, gh_url=''):
    rows = ''
    for _, r in top10_df.iterrows():
        rk  = int(r.get('Final_Rank', r.get('Rank', 0)))
        tk  = r['Ticker']
        cn  = r.get('CN_Name', '')
        sc  = r['New_Grok_Elite_Score']
        risk= r.get('風險等級', '')
        ret = r.get('1M_Return_pct', 0)
        pr  = r.get('Current_Price', 0)
        vr  = r.get('Volume_Ratio', 0)
        gap = r.get('Gap_Risk', '低')
        rsi = r.get('RSI', 0)
        rc  = '#30D158' if risk=='低' else ('#FF9F0A' if risk=='中' else '#FF453A')
        gc  = '#FF453A' if gap=='高' else ('#FF9F0A' if gap=='中' else '#30D158')
        rc2 = '#30D158' if ret >= 0 else '#FF453A'
        rows += (
            f'<tr style="border-bottom:1px solid #e0e0e0">'
            f'<td style="padding:8px;text-align:center;font-weight:bold">#{rk}</td>'
            f'<td style="padding:8px;font-weight:bold">{tk}</td>'
            f'<td style="padding:8px">{cn}</td>'
            f'<td style="padding:8px;text-align:center;font-weight:bold">{sc:.0f}</td>'
            f'<td style="padding:8px;text-align:center;color:{rc};font-weight:bold">{risk}</td>'
            f'<td style="padding:8px;text-align:right">${pr:.2f}</td>'
            f'<td style="padding:8px;text-align:right;color:{rc2}">{ret:+.1f}%</td>'
            f'<td style="padding:8px;text-align:center">{rsi:.0f}</td>'
            f'<td style="padding:8px;text-align:center">{vr:.2f}x</td>'
            f'<td style="padding:8px;text-align:center;color:{gc};font-weight:bold">{gap}</td>'
            f'</tr>'
        )
    link = f'<p>🔗 <a href="{gh_url}">點此開啟完整報告</a></p>' if gh_url else ''
    return (
        '<html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">'
        '<div style="max-width:800px;margin:0 auto;background:white;border-radius:12px;padding:24px">'
        f'<h2>📊 Grok Elite Swing — {today_str}</h2>'
        f'<p>SPY 1M {macro.get("spy_1m","N/A")} | QQQ 1M {macro.get("qqq_1m","N/A")}</p>'
        f'{link}'
        '<table style="width:100%;border-collapse:collapse;font-size:14px">'
        '<thead><tr style="background:#1a1a2e;color:white">'
        '<th style="padding:10px">排行</th><th>代碼</th><th>名稱</th>'
        '<th>評分</th><th>風險</th><th>股價</th>'
        '<th>1M漲幅</th><th>RSI</th><th>量比</th><th>Gap</th></tr></thead>'
        f'<tbody>{rows}</tbody></table>'
        '<p style="color:#999;font-size:12px">⚠️ 本報告為純技術面篩選，不構成投資建議。</p>'
        '</div></body></html>'
    )


def send_email(subject, html_body, attachments=None):
    user  = os.environ.get('GMAIL_USER', '')
    pwd   = os.environ.get('GMAIL_APP_PASSWORD', '')
    rcpts = os.environ.get('EMAIL_RECIPIENTS', '')
    if not user or not pwd or not rcpts:
        print('⚠️  Email 未設定，跳過')
        return
    recipients = [r.strip() for r in rcpts.split(',') if r.strip()]
    msg = MIMEMultipart('mixed')
    msg['From']    = f'Grok Elite Swing <{user}>'
    msg['To']      = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))
    for fp in (attachments or []):
        if not os.path.exists(fp):
            continue
        with open(fp,'rb') as fh:
            part = MIMEBase('application','octet-stream')
            part.set_payload(fh.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(fp)}"')
        msg.attach(part)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as sv:
            sv.login(user, pwd)
            sv.sendmail(user, recipients, msg.as_string())
        print(f'✅ Email 已發送至：{recipients}')
    except Exception as e:
        print(f'❌ Email 失敗：{e}')





# ════════════════════════════════════════════════════════════════
# ⑫ 主程式入口
# ════════════════════════════════════════════════════════════════
def main():
    # Step 1：建立股票池
    all_tickers = build_ticker_list()

    # Step 2：下載歷史資料
    price_data, spy_ret_1m, qqq_ret_1m = download_all(all_tickers)

    # Step 3：評分
    records = run_scoring(price_data, spy_ret_1m, qqq_ret_1m)

    if not records:
        print('⚠️ 今日沒有股票通過硬濾鏡，請檢查資料或調整門檻。')
        return

    # Step 4：宏觀快照
    print('\n📡 抓取宏觀數據...')
    macro = fetch_macro()
    print(
        f"📡 宏觀 | Brent ${macro.get('brent','N/A')} {macro.get('brent_note','')} "
        f"| SPY YTD {macro.get('spy_ytd','N/A')} 1M {macro.get('spy_1m','N/A')} "
        f"| QQQ YTD {macro.get('qqq_ytd','N/A')} 1M {macro.get('qqq_1m','N/A')}"
    )

    # Step 5：匯出
    results = export_results(records, price_data, macro)
    if results is None:
        return
    results_df, top10, f1, f2, f3 = results

    # Step 6：統計摘要
    print_summary(all_tickers, price_data, records, spy_ret_1m, qqq_ret_1m)

    # Step 7：發送通知
    gh_url = os.environ.get('PAGES_URL', '')
    send_telegram(build_telegram_message(top10, TODAY_STR, macro))
    send_email(
        subject     = f'📊 Grok Elite Swing Top10 — {TODAY_STR}',
        html_body   = build_email_html(top10, TODAY_STR, macro, gh_url),
        attachments = [f1, f2, f3]
    )
    print('\n✅ 所有通知發送完畢')


if __name__ == '__main__':
    main()
