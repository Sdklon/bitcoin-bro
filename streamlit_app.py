import streamlit as st
import pickle
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

from src.features.feature_generator import (
    generate_inference_df,
)

from src.trading.strategies import (
    generate_price_df,
    strategy_1,
    strategy_2,
    strategy_3,
    strategy_4,
)

DATA_DIR = Path.cwd() / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

MODEL_DIR = Path.cwd() / 'models'

BINANCE_HISTORICAL_DATA_DIR = RAW_DATA_DIR / 'binance_historical'
BINANCE_HISTORICAL_FILES_DIR = BINANCE_HISTORICAL_DATA_DIR / 'data/spot/daily/klines/BTCUSDT/1m'
BINANCE_HISTORICAL_DF_PATH = PROCESSED_DATA_DIR / 'binance_historical_df.csv'
BINANCE_PROCESSED_DF_PATH = PROCESSED_DATA_DIR / 'binance_processed_df.csv'
TRAIN_DF_PATH = PROCESSED_DATA_DIR / 'binance_train_df.csv'
VAL_DF_PATH = PROCESSED_DATA_DIR / 'binance_val_df.csv'
TEST_DF_PATH = PROCESSED_DATA_DIR / 'binance_test_df.csv'

TRADING_TYPE = 'spot'
TICKER_SYMBOL = 'BTCUSDT'
INTERVAL = '1m'
# No available data before 2021-03-01
START_DATE = '2021-03-01'
END_DATE = (datetime.utcnow() - timedelta(days=1) ).strftime('%Y-%m-%d')
# Reference: https://github.com/binance/binance-public-data/tree/master
RAW_DF_HEADERS = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

# Ensure directories are present
BINANCE_HISTORICAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

MA_WINDOW_SIZES_DICT = {
    "close_5m_ma": 5,
    "close_30m_ma": 30,
    "close_1h_ma": 60,
    "close_4h_ma": 240,
    "close_12h_ma": 720,
    "close_1d_ma": 1440,
    "close_15d_ma": 21600,
    "close_30d_ma": 43200,
}

MODEL_REGISTRY_DICT = {
    "XGBoost Baseline": str(MODEL_DIR / 'xgb_baseline.pkl')
}


# Functions
@st.cache(allow_output_mutation=True)
def load_model(model_type='XGBoost Baseline'):
    if model_type == 'XGBoost Baseline':
        model_path = str(MODEL_DIR / 'xgb_baseline.pkl')
        with open(model_path, 'rb') as f:
            model_package = pickle.load(f)
            
        encoder = model_package['ohe_encoder']
        model = model_package['model']

    else:
        raise ValueError(f"Unknown model being requested: {model_type}")

    return {'model': model, 'encoder': encoder}

def generate_data():
    # Load state
    if st.session_state['model'] is None:
        return

    start_date = (datetime.utcnow() - timedelta(days=30) ).strftime('%Y-%m-%d')
    end_date = (datetime.utcnow() - timedelta(days=1) ).strftime('%Y-%m-%d')

    processed_df = generate_inference_df(
        TRADING_TYPE,
        TICKER_SYMBOL,
        INTERVAL,
        start_date,
        end_date,
        BINANCE_HISTORICAL_DATA_DIR,
        BINANCE_HISTORICAL_FILES_DIR,
        str(PROCESSED_DATA_DIR / 'small_historical_for_realtime_df.csv'),
        RAW_DF_HEADERS,
        st.session_state['encoder'],
        MA_WINDOW_SIZES_DICT,
    )
    inference_X = processed_df.values[:, 1:-1]
    inference_Y = processed_df.values[:, -1]
    pred_inference_Y = st.session_state['model'].predict(inference_X)
    chart_df = pd.DataFrame({"Actual Price": inference_Y, "Predicted Price": pred_inference_Y})

    # Calculate trading profits
    price_df = generate_price_df(inference_Y, pred_inference_Y)
    strategy_profits = [strategy_1(price_df), strategy_2(price_df), strategy_3(price_df), strategy_4(price_df)]
    return chart_df, strategy_profits
    # plot_actual_and_predicted_price(inference_Y, pred_inference_Y, title="Actual and Predicted BTCUSDT Price for XGB Baseline")

# App layout
st.title("Bitcoin God")
st.markdown("Time-series prediction for BTCUSDT using data scraped from Binance API.  \n"
"Code for this project can be found [here](https://github.com/jonathanlimsc/bitcoin-god)"
)
# Init session states
st.session_state['model_type'] = "-"
st.session_state['encoder'] = None
st.session_state['model'] = None

with st.sidebar:
    model_type = st.selectbox("Select a model", MODEL_REGISTRY_DICT.keys())
    model_package = load_model(str(model_type))
    # Set state
    st.session_state['model_type'] = model_type
    st.session_state['model'] = model_package['model']
    st.session_state['encoder'] = model_package['encoder']

with st.container():
    st.subheader(f"Actual vs Predicted Price of BTCUSDT using {st.session_state['model_type']}")
    st.text(f"Data for {datetime.utcnow().strftime('%Y-%m-%d')} UTC time")
    chart_df, strategy_profits = generate_data()
    # Strategy profit metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Strategy 1 Profit", f"${strategy_profits[0]}")
    col2.metric("Strategy 2 Profit", f"${strategy_profits[1]}")
    col3.metric("Strategy 3 Profit", f"${strategy_profits[2]}")
    col4.metric("Strategy 4 Profit", f"${strategy_profits[3]}")
    
    if chart_df is not None:
        fig = px.line(chart_df)
        st.plotly_chart(fig, use_container_width=True)
    st.button("Pull New Data and Generate Predictions!")

with st.container():
    st.markdown("### Info about the trading strategies")
    st.markdown("Each trade is assumed to be for 1 BTC at prevailing price (close price for that minute). No trading fees are assumed.")
    st.markdown("#### Strategy 1")
    st.markdown("Buy if predicted price for that minute is greater than the previous minute's closing price. Sell on each minute's closing price. This strategy will never allow holding of the asset beyond a minute.")
    st.markdown("#### Strategy 2")
    st.markdown("Buy if predicted price for that minute is greater than the previous minute's closing price. Sell when that minute's predicted price is less than the buy price (buy price is assumed to be the closing price of the minute before the buy). Selling is only possible when a previous buy has taken place. This strategy allows holding of the asset across minutes, until the sell condition is triggered.")
    st.markdown("#### Strategy 3")
    st.markdown("Buy if predicted price for that minute is greater than the previous minute's predicted price. Sell on close. This strategy will never allow holding of the asset beyond a minute.")
    st.markdown("#### Strategy 4")
    st.markdown("Buy if predicted price for that minute is greater than the previous minute's predicted price. Sell when that minute's predicted price is less than the buy price (buy price is assumed to be the closing price of the minute before the buy). Selling is only possible when a previous buy has taken place. This strategy allows holding of the asset across minutes, until the sell condition is triggered.")