# # import pandas as pd
# # import yfinance as yf
# # import time
# # from datetime import datetime, timedelta

# # # file_path = r"C:\Users\81702\OneDrive\ドキュメント\stock list\nasdaqlisted.txt"

# # # =========================
# # # 読み込み
# # # =========================
# # # df = pd.read_csv(file_path, sep="|")


# # url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
# # df = pd.read_csv(url, sep="|")

# # # NaN除去（超重要）
# # df = df.dropna(subset=["Symbol"])

# # # 型統一（重要）
# # df["Symbol"] = df["Symbol"].astype(str)

# # # ゴミ銘柄除外
# # df = df[
# #     (df["ETF"] == "N") & 
# #     (~df["Symbol"].str.contains(r"W|U|R|\.", na=False))
# # ]

# # tickers = df["Symbol"].tolist()

# # # =========================
# # # 期間
# # # =========================
# # today = datetime.today()
# # one_year_ago = today - timedelta(days=365)
# # six_months_ago = today - timedelta(days=182)
# # one_month_ago = today - timedelta(days=30)
# # one_month_ago = today - timedelta(days=1)
# # start_of_year = datetime(today.year, 1, 1)

# # # =========================
# # # 関数
# # # =========================
# # def calc_return(df, start_date, end_date):
# #     try:
# #         df = df.dropna()

# #         start = df[df.index >= start_date]
# #         end = df[df.index <= end_date]

# #         if len(start) == 0 or len(end) == 0:
# #             return None

# #         return (end.iloc[-1]["Close"] / start.iloc[0]["Close"] - 1) * 100
# #     except:
# #         return None

# # # =========================
# # # 取得
# # # =========================
# # results = []
# # batch_size = 50

# # for i in range(0, len(tickers), batch_size):
# #     batch = tickers[i:i+batch_size]

# #     print(f"Processing {i} - {i+len(batch)}")

# #     try:
# #         data = yf.download(
# #             batch,
# #             start=one_year_ago.strftime("%Y-%m-%d"),
# #             end=today.strftime("%Y-%m-%d"),
# #             group_by="ticker",
# #             auto_adjust=True,
# #             progress=False,
# #             threads=False
# #         )

# #         for ticker in batch:
# #             try:
# #                 ticker_df = data[ticker] if len(batch) > 1 else data

# #                 name = df.loc[df["Symbol"] == ticker, "Security Name"].values[0]

# #                 results.append({
# #                     "Ticker": ticker,
# #                     "Name": name,
# #                     "1Y (%)": calc_return(ticker_df, one_year_ago, today),
# #                     "YTD (%)": calc_return(ticker_df, start_of_year, today),
# #                     "6M (%)": calc_return(ticker_df, six_months_ago, today),
# #                     "1M (%)": calc_return(ticker_df, one_month_ago, today),
# #                     "1D (%)": calc_return(ticker_df, one_day_ago, today),
# #                 })

# #             except:
# #                 continue

# #         # ★超重要：レート制限対策
# #         time.sleep(1)

# #     except:
# #         continue

# # result_df = pd.DataFrame(results)
# # result_df.to_csv("data.csv", index=False)

# # # =========================
# # # 表示
# # # =========================
# # def show_top(df, col):
# #     print(f"\n=== {col} Top 20 ===")
# #     print(df.dropna(subset=[col]).sort_values(by=col, ascending=False).head(20))



# # # =========================
# # # 上位20にSector追加
# # # =========================

# # def get_sector(ticker):
# #     try:
# #         info = yf.Ticker(ticker).info
# #         return info.get("sector", None)
# #     except:
# #         return None

# # def show_top_with_sector(df, col):
# #     print(f"\n=== {col} Top 20 (with Sector) ===")

# #     top_df = (
# #         df.dropna(subset=[col])
# #         .sort_values(by=col, ascending=False)
# #         .head(20)
# #         .copy()
# #     )

# #     sectors = []
# #     for i, ticker in enumerate(top_df["Ticker"]):
# #         print(f"Sector取得 {i}: {ticker}")
# #         sector = get_sector(ticker)
# #         sectors.append(sector)

# #         time.sleep(0.5)  # ←超重要（レート制限回避）

# #     top_df["Sector"] = sectors

# #     print(top_df[["Ticker", "Name", "Sector", col]])

# # # 実行
# # show_top_with_sector(result_df, "1Y (%)")
# # show_top_with_sector(result_df, "YTD (%)")
# # show_top_with_sector(result_df, "6M (%)")
# # show_top_with_sector(result_df, "1M (%)")
# # show_top_with_sector(result_df, "1D (%)")

# # # show_top(result_df, "1Y (%)")
# # # show_top(result_df, "YTD (%)")
# # # show_top(result_df, "6M (%)")
# # # show_top(result_df, "1M (%)")

# import pandas as pd
# import yfinance as yf
# import time
# from datetime import datetime, timedelta

# # =========================
# # NASDAQ銘柄取得（自動）
# # =========================
# url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
# df = pd.read_csv(url, sep="|")

# df = df.dropna(subset=["Symbol"])
# df["Symbol"] = df["Symbol"].astype(str)

# # ETF除外 + ゴミ銘柄除外
# df = df[
#     (df["ETF"] == "N") & 
#     (~df["Symbol"].str.contains(r"W|U|R|\.", na=False))
# ]

# tickers = df["Symbol"].tolist()

# # =========================
# # 期間
# # =========================
# today = datetime.today()
# one_year_ago = today - timedelta(days=365)
# six_months_ago = today - timedelta(days=182)
# one_month_ago = today - timedelta(days=30)
# start_of_year = datetime(today.year, 1, 1)

# # =========================
# # 関数
# # =========================

# # 通常リターン（期間指定）
# def calc_return(df, start_date, end_date):
#     try:
#         df = df.dropna()

#         start = df[df.index >= start_date]
#         end = df[df.index <= end_date]

#         if len(start) == 0 or len(end) == 0:
#             return None

#         return (end.iloc[-1]["Close"] / start.iloc[0]["Close"] - 1) * 100
#     except:
#         return None

# # 1営業日前リターン（重要）
# def calc_1d_return(df):
#     try:
#         df = df.dropna()

#         if len(df) < 2:
#             return None

#         return (df.iloc[-1]["Close"] / df.iloc[-2]["Close"] - 1) * 100
#     except:
#         return None

# # =========================
# # データ取得
# # =========================
# results = []
# batch_size = 50

# for i in range(0, len(tickers), batch_size):
#     batch = tickers[i:i+batch_size]

#     print(f"Processing {i} - {i+len(batch)}")

#     try:
#         data = yf.download(
#             batch,
#             start=one_year_ago.strftime("%Y-%m-%d"),
#             end=today.strftime("%Y-%m-%d"),
#             group_by="ticker",
#             auto_adjust=True,
#             progress=False,
#             threads=False
#         )

#         for ticker in batch:
#             try:
#                 ticker_df = data[ticker] if len(batch) > 1 else data

#                 name = df.loc[df["Symbol"] == ticker, "Security Name"].values[0]

#                 results.append({
#                     "Ticker": ticker,
#                     "Name": name,
#                     "1Y (%)": calc_return(ticker_df, one_year_ago, today),
#                     "YTD (%)": calc_return(ticker_df, start_of_year, today),
#                     "6M (%)": calc_return(ticker_df, six_months_ago, today),
#                     "1M (%)": calc_return(ticker_df, one_month_ago, today),
#                     "1D (%)": calc_1d_return(ticker_df),
#                 })

#             except:
#                 continue

#         # レート制限対策
#         time.sleep(1)

#     except:
#         continue

# result_df = pd.DataFrame(results)
# result_df.to_csv("data.csv", index=False)

# # =========================
# # 表示関数
# # =========================
# def show_top(df, col):
#     print(f"\n=== {col} Top 20 ===")
#     print(
#         df.dropna(subset=[col])
#         .sort_values(by=col, ascending=False)
#         .head(20)
#     )

# # =========================
# # Sector取得（遅いのでTopのみ）
# # =========================
# def get_sector(ticker):
#     try:
#         info = yf.Ticker(ticker).info
#         return info.get("sector", None)
#     except:
#         return None

# def show_top_with_sector(df, col):
#     print(f"\n=== {col} Top 20 (with Sector) ===")

#     top_df = (
#         df.dropna(subset=[col])
#         .sort_values(by=col, ascending=False)
#         .head(20)
#         .copy()
#     )

#     sectors = []
#     for i, ticker in enumerate(top_df["Ticker"]):
#         print(f"Sector取得 {i}: {ticker}")
#         sector = get_sector(ticker)
#         sectors.append(sector)

#         time.sleep(0.5)

#     top_df["Sector"] = sectors

#     print(top_df[["Ticker", "Name", "Sector", col]])

# # =========================
# # 実行
# # =========================
# show_top_with_sector(result_df, "1Y (%)")
# show_top_with_sector(result_df, "YTD (%)")
# show_top_with_sector(result_df, "6M (%)")
# show_top_with_sector(result_df, "1M (%)")
# show_top_with_sector(result_df, "1D (%)")

#test

import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta

# =========================
# NASDAQ銘柄取得（自動）
# =========================
url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
df = pd.read_csv(url, sep="|")

df = df.dropna(subset=["Symbol"])
df["Symbol"] = df["Symbol"].astype(str)

# ETF除外 + ゴミ銘柄除外
df = df[
    (df["ETF"] == "N") & 
    (~df["Symbol"].str.contains(r"W|U|R|\.", na=False))
]

tickers = df["Symbol"].tolist()
tickers = tickers[:100]

# =========================
# 期間
# =========================
today = datetime.today()
one_year_ago = today - timedelta(days=365)
six_months_ago = today - timedelta(days=182)
one_month_ago = today - timedelta(days=30)
start_of_year = datetime(today.year, 1, 1)

# =========================
# 関数
# =========================

# 通常リターン（期間指定）
def calc_return(df, start_date, end_date):
    try:
        df = df.dropna()

        start = df[df.index >= start_date]
        end = df[df.index <= end_date]

        if len(start) == 0 or len(end) == 0:
            return None

        return (end.iloc[-1]["Close"] / start.iloc[0]["Close"] - 1) * 100
    except:
        return None

# 1営業日前リターン（重要）
def calc_1d_return(df):
    try:
        df = df.dropna()

        if len(df) < 2:
            return None

        return (df.iloc[-1]["Close"] / df.iloc[-2]["Close"] - 1) * 100
    except:
        return None

# =========================
# データ取得
# =========================
results = []
batch_size = 100

for i in range(0, len(tickers), batch_size):
    batch = tickers[i:i+batch_size]

    print(f"Processing {i} - {i+len(batch)}")

    try:
        data = yf.download(
            batch,
            start=one_year_ago.strftime("%Y-%m-%d"),
            end=today.strftime("%Y-%m-%d"),
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=False
        )

        for ticker in batch:
            try:
                ticker_df = data[ticker] if len(batch) > 1 else data

                name = df.loc[df["Symbol"] == ticker, "Security Name"].values[0]

                results.append({
                    "Ticker": ticker,
                    "Name": name,
                    "1Y (%)": calc_return(ticker_df, one_year_ago, today),
                    "YTD (%)": calc_return(ticker_df, start_of_year, today),
                    "6M (%)": calc_return(ticker_df, six_months_ago, today),
                    "1M (%)": calc_return(ticker_df, one_month_ago, today),
                    "1D (%)": calc_1d_return(ticker_df),
                })

            except:
                continue

        # レート制限対策
        time.sleep(0.3)

    except:
        continue

result_df = pd.DataFrame(results)
result_df.to_csv("data.csv", index=False)

# =========================
# 表示関数
# =========================
def show_top(df, col):
    print(f"\n=== {col} Top 20 ===")
    print(
        df.dropna(subset=[col])
        .sort_values(by=col, ascending=False)
        .head(20)
    )

# =========================
# Sector取得（遅いのでTopのみ）
# =========================
def get_sector(ticker):
    try:
        info = yf.Ticker(ticker).info
        return info.get("sector", None)
    except:
        return None

def show_top_with_sector(df, col):
    print(f"\n=== {col} Top 20 (with Sector) ===")

    top_df = (
        df.dropna(subset=[col])
        .sort_values(by=col, ascending=False)
        .head(20)
        .copy()
    )

    sectors = []
    for i, ticker in enumerate(top_df["Ticker"]):
        print(f"Sector取得 {i}: {ticker}")
        sector = get_sector(ticker)
        sectors.append(sector)

        time.sleep(0.5)

    top_df["Sector"] = sectors

    print(top_df[["Ticker", "Name", "Sector", col]])

# =========================
# 実行
# =========================
show_top_with_sector(result_df, "1Y (%)")
show_top_with_sector(result_df, "YTD (%)")
show_top_with_sector(result_df, "6M (%)")
show_top_with_sector(result_df, "1M (%)")
show_top_with_sector(result_df, "1D (%)")

