import pandas as pd


class DataCleaner:
    """
    A class used to clean data.

    ...

    Methods
    -------
    remove_unwanted_columns(df, columns)
        removes columns from a dataframe
    remove_nulls(df)
        removes null values from a dataframe
    remove_duplicates(df)
        removes duplicate rows from a dataframe
    missing_percentage(df)
        returns the null value percentage of a column

    """

    def __init__(self) -> None:
        pass

    def remove_unwanted_columns(self,  df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Removes list of columns from the given dataframe.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe from which we are removing columns
        columns : list
            The list of columns we are removing
        """

        df.drop(columns, axis=1, inplace=True)
        return df

    def remove_nulls(self,  df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes null values for a dataframe.

        Parameters
        ----------
        df: pd.DataFrame
            The dataframe for which we are removing nulls
        """

        return df.dropna()

    def remove_duplicates(self,  df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes duplicate records from a dataframe.

        Parameters
        ----------
        df: pd.DataFrame
            The dataframe for which we are removing duplicates
        """

        remove = df[df.duplicated()].index
        return df.drop(index=remove, inplace=True)

    def missing_percentage(self, df: pd.DataFrame):
        """
        Returns the missing percentage of a column.

        Parameters
        ----------
        df: pd.DataFrame
            The dataframe to get the missing percentages from
        """

        percent_missing = df.isnull().sum() * 100 / len(df)
        missing_value_df = pd.DataFrame(
            {"column_name": df.columns, "percent_missing": percent_missing}
        )
        missing_value_df.reset_index(drop=True, inplace=True)
        return missing_value_df