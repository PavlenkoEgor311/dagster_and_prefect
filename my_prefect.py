from prefect import task, flow
import pandas as pd
from urllib.parse import urlparse

pathIn, pathOut = "original.csv", "norm.csv"


@task
def loadData(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


@task
def getDomain(df: pd.DataFrame) -> pd.DataFrame:
    df['domain'] = [urlparse(i).netloc for i in df['url']]
    return df


@task
def uploadData(df: pd.DataFrame, path: str):
    df.to_csv(path)


@flow(name="task_prefect")
def run():
    data = loadData(path=pathIn)
    dataDomains = getDomain(df=data)
    uploadData(df=dataDomains, path=pathOut)


if __name__ == '__main__':
    run()
