import pandas as pd
import numpy as np


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

    def percent_missing(self, df: pd.DataFrame):
        """
        This function takes the dataframe and prints the percentage of
        missing values

        Args:
        -----
        df: dataframe
        """

        # Calculate total number of cells in dataframe
        total_cells = np.product(df.shape)

        # Count number of missing values per column
        missing_count = df.isnull().sum()

        # Calculate total number of missing values
        total_missing = missing_count.sum()

        # Calculate percentage of missing values
        print(round(((total_missing / total_cells) * 100), 2), "%", "missing values.")

    def missing_values_table(self, df: pd.DataFrame):
        """
        This function takes the dataframe and calculate
        missing values by column

        Args:
        -----
        df: dataframe
        """

        # Total missing values
        mis_val = df.isnull().sum()

        # Percentage of missing values
        mis_val_percent = 100 * df.isnull().sum() / len(df)

        # dtype of missing values
        mis_val_dtype = df.dtypes

        # Make a table with the results
        mis_val_table = pd.concat([mis_val, mis_val_percent, mis_val_dtype], axis=1)

        # Rename the columns
        mis_val_table_ren_columns = mis_val_table.rename(
            columns={0: 'Missing Values', 1: '% of Total Values', 2: 'Dtype'})

        # Sort the table by percentage of missing descending
        mis_val_table_ren_columns = mis_val_table_ren_columns[
            mis_val_table_ren_columns.iloc[:, 1] != 0].sort_values(
            '% of Total Values', ascending=False).round(1)

        # Print some summary information
        print("Your selected dataframe has " + str(df.shape[1]) + " columns.\n"
                                                                  "There are " + str(
            mis_val_table_ren_columns.shape[0]) +
              " columns that have missing values.")

        # Return the dataframe with missing information
        return mis_val_table_ren_columns