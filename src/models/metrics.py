from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    mean_absolute_percentage_error,
)

def get_metrics(y_true, y_pred, print_metrics=True):
    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    metrics = {
        "mean_squared_error": mse,
        "mean_absolute_error": mae,
        "mean_absolute_percentage_error": mape,
    }
    
    if print_metrics:
        print(metrics)
        
    return metrics