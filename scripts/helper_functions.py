import numpy as np


# root mean square percentage error
def rmspe(y, yhat):
    rmspe = np.sqrt(np.mean((y - yhat) ** 2))
    return rmspe


#
def data_normalize(data):
    return (data - data.min()) / (data.max() - data.min())
