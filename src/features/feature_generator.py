import pandas as pd
from datetime import datetime
from sklearn.preprocessing import OneHotEncoder
from typing import Tuple

from .utilities import (
    convert_unix_time_to_day_of_week,
    convert_unix_time_to_hour,
    convert_unix_time_to_hour_quarter,
    convert_unix_time_to_human_time_string,
    convert_unix_time_to_month,
)

from ..data.binance_downloader import (
    get_realtime_klines,
    generate_latest_historical_df,
)

def generate_moving_average_features(raw_df: pd.DataFrame, ma_window_sizes_dict: dict, feature: str = 'close') -> pd.DataFrame:
    """
    Generate moving average features

    Args:
        raw_df: Original dataframe
        ma_windows_sizes_dict: Dictionary containing moving-average feature name as key, moving average window size as value
        feature: Name of feature column to compute moving average over
    """
    df = raw_df.copy()
    shifted_feature_series = df[feature].shift(1)
    for new_feature_name, window_size in ma_window_sizes_dict.items():
        # Shift 1 as we are calculating moving averages from previous values
        df[new_feature_name] = shifted_feature_series.rolling(window_size).mean()
    return df

def generate_lag_features(raw_df: pd.DataFrame, feature: str = 'close', max_offset_period: int = 120) -> pd.DataFrame:
    """
    Generate lagged features

    Args:
        raw_df: Original dataframe
        feature: Name of feature column to compute moving average over
        max_offset_period: Number of periods to generate lagged features (e.g. 120 => features are generated for period t-1 to t-120)
    """
    df = raw_df.copy()
    columns = []
    series = []
    # To avoid PerformanceWarning, we use pd.concat to combine all the series
    for offset_period in range(1, max_offset_period+1):
        columns.append(f"{feature}_t_minus_{offset_period}")
        series.append(df[feature].shift(offset_period))
        # df[f"{feature}_t_minus_{offset_period}"] = df[feature].shift(offset_period)

    lag_df = pd.DataFrame(pd.concat(series, axis=1))
    lag_df.columns = columns
    df = pd.concat([df, lag_df], axis=1)
    
    return df
    
def generate_time_features(raw_df: pd.DataFrame, time_column: str = 'close_time', generate_human_time: bool = False) -> pd.DataFrame:
    """
    Generate time features

    Args:
        raw_df: Original dataframe
        time_column: Column that contains Unix time
        generate_human_time: Whether to generate human readable time column. Defaults to False.
    """
    df = raw_df.copy()
    df['day_of_week'] = df[time_column].apply(convert_unix_time_to_day_of_week)
    df['month_of_year'] = df[time_column].apply(convert_unix_time_to_month)
    df['hr_of_day'] = df[time_column].apply(convert_unix_time_to_hour)
    df['quarter_of_hour'] = df[time_column].apply(convert_unix_time_to_hour_quarter)
    if generate_human_time:
        df['human_time'] = df[time_column].apply(convert_unix_time_to_human_time_string)
    return df

def feature_pipeline_v1(raw_df: pd.DataFrame, ma_window_sizes_dict: dict, lag_max_offset_period: int = 120, cols_to_remove: list = ['open_time', 'open', 'high', 'low', 'volume', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'], ohe_encoder=None, ohe_columns=['day_of_week', 'month_of_year', 'hr_of_day', 'quarter_of_hour']) -> Tuple[pd.DataFrame, OneHotEncoder]:
    """
    V1 feature pipeline that generates all the relevant features for training

    Args:
        raw_df: Original dataframe
        ma_windows_sizes_dict: Dictionary containing moving-average feature name as key, moving average window size as value
        lag_max_offset_period: Number of periods to generate lagged features (e.g. 120 => features are generated for period t-1 to t-120)
        cols_to_remove: Remove these columns that are not used
        ohe_encoder: One-hot encoder can be optionally provided for transformation of feature columns to one-hot encoding. If None, a new one will be instantiated.
        ohe_columns: Columns to transform via one-hot encoding

    """
    df = raw_df.copy()
    df = generate_moving_average_features(df, ma_window_sizes_dict, feature='close')
    df = generate_lag_features(df, feature='close', max_offset_period=lag_max_offset_period)
    df = generate_lag_features(df, feature='volume', max_offset_period=lag_max_offset_period)
    df = generate_time_features(df, time_column='close_time', generate_human_time=False)
    
    if len(cols_to_remove) > 0:
        df = df.drop(columns=cols_to_remove)
    
    # Remove NAs
    df = df.dropna()
    
    df = df.reset_index(drop=True)
    
    # Generate one-hot encodings for ohe_columns
    if ohe_encoder is None:
        ohe_encoder = OneHotEncoder()
        ohe_encoder.fit(df[ohe_columns])
        
    ohe_features = ohe_encoder.transform(df[ohe_columns]).toarray()
    ohe_df = pd.DataFrame(ohe_features, columns=ohe_encoder.get_feature_names_out())
    df = pd.concat([df.drop(columns=ohe_columns), ohe_df], axis=1)
    
    # Fix order
    feature_cols = list(df.columns)
    feature_cols.remove('close_time')
    feature_cols.remove('close')
    feature_cols = ['close_time'] + feature_cols + ['close']
    
    df = df.reset_index(drop=True)
    
    return df[feature_cols], ohe_encoder

# Prepare data for inference
# Take data from 30 days ago to support all required feature generation
def generate_inference_df(trading_type,
                          ticker_symbol,
                          interval,
                          start_date,
                          end_date,
                          historical_data_dir,
                          historical_files_dir,
                          small_historical_df_path,
                          raw_df_headers,
                          ohe_encoder,
                          ma_window_sizes_dict
                         ):
    historical_df = generate_latest_historical_df(trading_type, 
                                                  ticker_symbol,
                                                  interval, 
                                                  start_date, 
                                                  end_date, 
                                                  historical_data_dir, 
                                                  historical_files_dir, 
                                                  small_historical_df_path, 
                                                  raw_df_headers,
                                                  write_csv=True
                                                 )
    historical_end_time = int(historical_df.iloc[-1]['close_time'])
    realtime_klines = get_realtime_klines(start_time=historical_end_time + 1, ticker=ticker_symbol, interval=interval)
    realtime_df = pd.DataFrame(realtime_klines, columns=raw_df_headers).apply(pd.to_numeric)
    combined_df = pd.concat([historical_df, realtime_df], axis=0)
    processed_df, ohe_encoder = feature_pipeline_v1(combined_df, ma_window_sizes_dict, lag_max_offset_period=120, ohe_encoder=ohe_encoder)
    
    return processed_df