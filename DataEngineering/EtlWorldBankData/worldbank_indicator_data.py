import pandas as pd
import requests
import datetime
from typing import List, Dict, Any
import sys
sys.path.append("/home/hynso/Documents/Learning/Udacity")

from DataEngineering.EtlWorldBankData.iso_country_code_data import get_iso_country_code_df


# Define helper functions

def get_total_record_counts(
        indicator_codes_dict: Dict[str, str],
        api_url_stub: str,
        api_params: Dict[str, Any]
) -> Dict[str, int]:
    """
    Returns a dict of the total number of records for given World Bank data indicator codes,
    a World Bank Data API URL stub, and World Bank Data API parameters

    :param indicator_codes_dict: dict of indicator codes (keys) and indicator descriptions (values)
    :param api_url_stub: World Bank API URL stub to be augmented in-function (https://tinyurl.com/ya2ohwbw)
    :param api_params: dict of API parameters to pass to requests.get()
    :return: dict of indicator codes (keys) and total records (values)
    """
    total_records_dict: Dict = {}
    api_params["per_page"]: int = 1

    for indicator_code in indicator_codes_dict.keys():
        api_url: str = f"{api_url_stub}/{indicator_code}/"
        print(f"Retrieving record count for indicator {indicator_code} from {api_url}")
        r = requests.get(api_url, params=api_params)
        total_records_dict[indicator_code]: int = int(r.json()[0]["total"])

    return total_records_dict


def get_dataframes(
        total_records_dict: Dict[str, int],
        api_url_stub: str,
        api_params: Dict,
        country_codes: List[str]
) -> Dict[str, pd.DataFrame]:
    """
    Returns a dict of pandas DataFrames for a given total records dict,
    a World Bank Data API URL stub, and World Bank Data API parameters

    :param total_records_dict:
    :param api_url_stub: World Bank API URL stub to be augmented in-function (https://tinyurl.com/ya2ohwbw)
    :param api_params: dict of API parameters to pass to requests.get()
    :param country_codes: list of ISO 3 country codes (e.g., Afghanistan = AFG)
    :return: dict of indicator codes (keys) and corresponding pandas DataFrames (values)
    """
    dfs_dict: Dict[str, pd.DataFrame] = {}
    country_codes: List[str] = list(country_codes)

    for indicator_code, total_records in total_records_dict.items():
        api_params["per_page"]: int = total_records
        api_url: str = f"{api_url_stub}/{indicator_code}"
        print(f"Retrieving data for indicator {indicator_code} from {api_url}")
        r = requests.get(api_url, params=api_params)
        dfs_dict[indicator_code] = (pd.DataFrame.from_dict(r.json()[1])
                                    .loc[:, ["countryiso3code", "date", "value"]]
                                    .rename(columns={"countryiso3code": "iso_code_3", "date": "year"})
                                    .query('iso_code_3 in @country_codes'))

    return dfs_dict


def expand_df_dict_values(
        df: pd.DataFrame,
        cols_to_expand: List[str]
) -> pd.DataFrame:
    """
    Expands the dict-like values in columns provided by the user

    :param df: pandas DataFrame with columns that contain dict-like values
    :param cols_to_expand: list of columns to expand
    :return: pandas DataFrame with expanded columns on the left
    """
    expanded_df = df.copy(deep=True)

    for col in cols_to_expand:
        expanded_df = pd.concat([expanded_df[col].apply(pd.Series), expanded_df.drop(columns=[col])], axis="columns")

    return expanded_df


def expand_dataframes(
        dfs_dict: Dict[str, pd.DataFrame],
        cols_to_expand: List[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Applies `expand_df_dict_values` to every DataFrame in `dfs_dict`
    Note: All DataFrames in `dfs_dict` must have the same columns to expand

    :param dfs_dict: dict that contains DataFrames with columns to expand
    :param cols_to_expand: list of columns to expand
    :return: dict of DataFrames with expanded columns
    """
    if cols_to_expand is None:
        return dfs_dict

    expanded_dfs_dict = {}
    for key, df in dfs_dict.items():
        expanded_dfs_dict[key] = expand_df_dict_values(df, cols_to_expand)

    return expanded_dfs_dict


def get_worldbank_indicator_dfs_dict(
        worldbank_indicator_code_dict: Dict[str, str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Get World Bank indicator dataframes

    :param worldbank_indicator_code_dict: dict of Worldbank Indicator data codes and their descriptions
    :return: dict of pandas DataFrames with
    """
    if worldbank_indicator_code_dict is None:
        worldbank_indicator_code_dict: Dict[str, str] = {
            "IT.NET.SECR": "Secure Internet servers",
            "IT.NET.SECR.P6": "Secure Internet servers (per 1 million people)",
            "IT.NET.USER.ZS": "Individuals using the Internet (% of population)",
            "MS.MIL.TOTL.P1": "Armed forces personnel, total",
            "MS.MIL.TOTL.TF.ZS": "Armed forces personnel (% of total labor force)",
            "MS.MIL.XPND.CN": "Military expenditure (current LCU)",
            "MS.MIL.XPND.GD.ZS": "Military expenditure (% of GDP)",
            "MS.MIL.XPND.ZS": "Military expenditure (% of central government expenditure)",
            "SP.POP.TOTL": "Population, total",
        }

    countries: str = "all"
    min_year: int = 2012  # First year of OONI
    max_year: int = datetime.date.today().year
    worldbank_api_url_stub: str = f"https://api.worldbank.org/v2/country/{countries}/indicator"
    worldbank_record_api_params: Dict[str, str] = {"date": f"{min_year}:{max_year}", "format": "json"}
    df_country_codes = get_iso_country_code_df()

    worldbank_total_record_counts_dict = get_total_record_counts(
        indicator_codes_dict=worldbank_indicator_code_dict,
        api_url_stub=worldbank_api_url_stub,
        api_params=worldbank_record_api_params
    )

    worldbank_indicator_dfs_dict = get_dataframes(
        total_records_dict=worldbank_total_record_counts_dict,
        api_url_stub=worldbank_api_url_stub,
        api_params=worldbank_record_api_params,
        country_codes=df_country_codes.iso_code_3
    )

    expanded_worldbank_indicator_dfs_dict = expand_dataframes(
        dfs_dict=worldbank_indicator_dfs_dict,
        cols_to_expand=[]
    )

    return expanded_worldbank_indicator_dfs_dict
