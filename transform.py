import pandas as pd
import load


def get_clean_taxi_data():
    """
    clean pd.DataFrame
    :return:
    """
    df = load.load_taxi_data()
    # clean
    return df


def get_clean_covid_data():
    """
    clean pd.DataFrame
    :return:
    """
    df = load.load_covid_data()
    # clean
    return df


def get_clean_weather_data():
    """
    clean pd.DataFrame
    :return:
    """
    df = load.load_weather_data()
    # clean
    return df


def run():
    pass


if __name__ == '__main__':
    run()
