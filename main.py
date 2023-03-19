from dagster import asset, materialize, op
import pandas as pd
from urllib.parse import urlparse

pathIn, pathOut = "original.csv", "norm.csv"


def fixPath(inF, outF):
    global pathIn, pathOut
    pathIn = inF
    pathOut = outF


@asset
def loadData():
    df = pd.read_csv(pathIn)
    return df


@asset
def getDomain(loadData):
    loadData['domain'] = [urlparse(i).netloc for i in loadData['url']]
    return loadData


@asset
def uploadData(getDomain):
    getDomain.to_csv(pathOut)


def run():
    materialize(assets=[loadData, getDomain, uploadData])


if __name__ == '__main__':
    run()
