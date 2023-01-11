import pandas as pd

def generate_price_df(actual_Y, pred_Y):
    price_df = pd.DataFrame({'actual': actual_Y, 'predicted': pred_Y})
    price_df['prev_actual'] = price_df['actual'].shift(1)
    price_df['prev_predicted'] = price_df['predicted'].shift(1)
    price_df = price_df.dropna()
    return price_df

def strategy_1(price_df):
    # Strategy 1: Buy if predicted > prev_actual. Sell on close. Profit = Actual - prev_actual
    total_profit = 0

    for idx, row in price_df.iterrows():
        # Buy (assume entry price is prev_actual) and sell on close
        if row['predicted'] > row['prev_actual']:
            profit = row['actual'] - row['prev_actual']
        total_profit += profit
    
    return round(total_profit, 2)

def strategy_2(price_df):
    # Strategy 2: Buy if predicted > prev_actual. Sell when predicted < buy_price. Profit = sell_price - buy_price
    total_profit = 0
    buy_price = 0
    
    for idx, row in price_df.iterrows():
        # Buy (assume entry price is prev_actual)
        if buy_price == 0 and row['predicted'] > row['prev_actual']:
            buy_price = row['actual']
        # Sell (if previously bought and price is predicted to be higher than buy_price)
        elif buy_price > 0 and row['predicted'] > buy_price:
            profit = row['actual'] - buy_price
            # Reset buy_price
            buy_price = 0
            total_profit += profit

    return round(total_profit, 2)

def strategy_3(price_df):
    # Strategy 3: Buy if predicted > prev_predicted. Sell on close.
    total_profit = 0

    for idx, row in price_df.iterrows():
        # Buy (assume entry price is prev_actual) and sell on close
        if row['predicted'] > row['prev_predicted']:
            profit = row['actual'] - row['prev_actual']
            total_profit += profit
    
    return round(total_profit, 2)

def strategy_4(price_df):
    # Strategy 4: Buy if predicted > prev_predicted. Sell if predicted < prev_predicted.
    total_profit = 0
    buy_price = 0
    for idx, row in price_df.iterrows():
        # Buy (assume entry price is prev_actual)
        if buy_price == 0 and row['predicted'] > row['prev_predicted']:
            buy_price = row['prev_actual']
        elif buy_price > 0 and row['predicted'] < row['prev_predicted']:
            profit = row['actual'] - buy_price
            # Reset buy_price
            buy_price = 0
            total_profit += profit
    
    return round(total_profit, 2)