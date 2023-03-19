from prefect import task, flow
import pandas as pd
from urllib.parse import urlparse


@task
def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


@task
def get_domain(df: pd.DataFrame) -> pd.DataFrame:
    df['domain'] = [urlparse(i).netloc for i in df['url']]
    return df


@task
def upload_data(df: pd.DataFrame, path: str):
    df.to_csv(path)


@flow(name="task_prefect")
def my_flow(path_in, path_out):
    data = load_data(path=path_in)
    data_domains = get_domain(df=data)
    upload_data(df=data_domains, path=path_out)