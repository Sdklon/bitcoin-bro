import numpy as np
import matplotlib.pyplot as plt

def plot_actual_and_predicted_price(test_Y: np.ndarray, pred_test_Y: np.ndarray, title: str = None):
    fig, ax = plt.subplots(figsize=(8,6))
    ax.plot(test_Y, label='Actual Price', linewidth=0.5)
    ax.plot(pred_test_Y, label='Predicted Price', alpha=0.8, linewidth=0.5)
    ax.set_ylabel('Price')
    ax.set_xlabel('Minute in Day')
    ax.legend()
    if title:
        ax.set_title(title)
        
    fig.show()