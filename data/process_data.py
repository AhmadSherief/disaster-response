# import libraries
import pandas as pd
from sqlalchemy import create_engine
import sys


def load_data(messages_filepath, categories_filepath):
    """ Loads the data from specified file paths and 
        merges them into one DataFrame"""

    # load messages dataset
    messages = pd.read_csv(messages_filepath)

    # load categories dataset
    categories = pd.read_csv(categories_filepath)

    # merge datasets
    df = pd.merge(messages, categories, on="id")

    return df


def clean_data(df):
    """
    Cleans the data by creating column for each category found in the categories column
    and removes duplicate entries
    """
    # Split the values in the categories column on the ; character
    # so that each value becomes a separate column

    # create a dataframe of the 36 individual category columns
    categories = df["categories"].str.split(";", expand=True)

    # select the first row of the categories dataframe
    row = categories.loc[0]

    # use this row to extract a list of new column names for categories.
    # one way is to apply a lambda function that takes everything
    # up to the second to last character of each string with slicing
    category_colnames = row.apply(lambda x: x[:-2])

    # rename the columns of `categories`
    categories.columns = category_colnames

    # Convert category values to just numbers 0 or 1, which is the last
    # character in the column name
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]

        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column].astype(str))

    # Store only 0 or 1 in related column
    categories["related"] = categories["related"].apply(lambda x: 0 if x == 0 else 1)

    # drop columns with no variation in its data.
    categories.drop(["child_alone"], inplace=True, axis=1)

    # Replace categories column in df with new category columns
    # drop the original categories column from `df`
    df.drop(["categories"], axis=1, inplace=True)

    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)

    # drop duplicates
    df.drop_duplicates(inplace=True)

    return df


def save_data(df, database_filename):
    """ Save the clean dataset into an sqlite database"""
    engine = create_engine("sqlite:///{}".format(database_filename))
    df.to_sql("appen", engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print(
            "Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}".format(
                messages_filepath, categories_filepath
            )
        )
        df = load_data(messages_filepath, categories_filepath)

        print("Cleaning data...")
        df = clean_data(df)

        print("Saving data...\n    DATABASE: {}".format(database_filepath))
        save_data(df, database_filepath)

        print("Cleaned data saved to database!")

    else:
        print(
            "Please provide the filepaths of the messages and categories "
            "datasets as the first and second argument respectively, as "
            "well as the filepath of the database to save the cleaned data "
            "to as the third argument. \n\nExample: python process_data.py "
            "disaster_messages.csv disaster_categories.csv "
            "DisasterResponse.db"
        )


if __name__ == "__main__":
    main()
