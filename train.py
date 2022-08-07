import dvc.api
import mlflow
import pandas as pd

path = "data/campaigns_inventory_updated.csv"
repo = "https://github.com/DiyeMark/AdludioDataScienceChallenge"
version = None

# data_url = dvc.api.get_url(path=path, repo=repo, rev=version)

if __name__ == "__main___":
    mlflow.set_experiment("Adludio DataScience Challenge")

    # data = pd.read_csv(data_url, sep=",")

    # mlflow.log_param("data_url", data_url)
    # mlflow.log_param("data_version", version)

    # mlflow.log_artifact("data/campaigns_inventory_updated.csv")
