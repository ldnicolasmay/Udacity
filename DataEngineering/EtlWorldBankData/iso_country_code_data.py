import pandas as pd


def get_iso_country_code_df() -> pd.DataFrame:
    """
    Returns pandas DataFrame of ISO 2-char and 3-char country codes

    :return: pandas DataFrame of ISO 2-char and 3-char country codes
    """
    iso_country_code_csv_url: str = \
        "https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/raw/master/all/all.csv"
    iso_country_code_df: pd.DataFrame = (pd.read_csv(iso_country_code_csv_url, usecols=["name", "alpha-2", "alpha-3"])
                                         .rename(columns={"alpha-2": "iso_code_2", "alpha-3": "iso_code_3"}))

    return iso_country_code_df
