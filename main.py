from dagster import asset, materialize, op
import pandas as pd
from urllib.parse import urlparse

path_in, path_out = "original.csv", "norm.csv"


def fix_path(f_in, f_out):
    global path_in, path_out
    path_in = f_in
    path_out = f_out


@asset
def load_data():
    df = pd.read_csv(path_in)
    return df


@asset
def get_domain(load_data):
    load_data['domain'] = [urlparse(i).netloc for i in load_data['url']]
    return load_data


@asset
def upload_data(get_domain):
    get_domain.to_csv(path_out)


def run_assets():
    materialize(assets=[load_data, get_domain, upload_data])


if __name__ == '__main__':
    run_assets()
