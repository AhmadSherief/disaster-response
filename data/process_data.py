# import libraries
import pandas as pd
from sqlalchemy import create_engine

# load messages dataset
messages = pd.read_csv('messages.csv')

# load categories dataset
categories = pd.read_csv('categories.csv')

# merge datasets
df = pd.merge(messages, categories, on='id')

# Split the values in the categories column on the ; character
# so that each value becomes a separate column

# create a dataframe of the 36 individual category columns
categories = df['categories'].str.split(';',expand=True)

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


# Replace categories column in df with new category columns

# drop the original categories column from `df`
df.drop(['categories'],axis=1,inplace = True)

# concatenate the original dataframe with the new `categories` dataframe
df = pd.concat([df, categories], axis=1)

# drop duplicates
df.drop_duplicates(inplace=True)

# Save the clean dataset into an sqlite database called appen.db
engine = create_engine('sqlite:///appen.db')
df.to_sql('appen', engine, index=False)