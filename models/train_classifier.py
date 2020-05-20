# import libraries
import sys
import numpy as np
import pandas as pd

import nltk

nltk.download(["punkt", "wordnet", "averaged_perceptron_tagger"])


from sqlalchemy import create_engine
from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer
import re

from sklearn.model_selection import GridSearchCV
from sklearn.svm import LinearSVC
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.multioutput import MultiOutputClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.metrics import classification_report
import pickle


def load_data(database_filepath):
    """ load data from database and define feature and target variables X and Y"""

    # create engine to get connection
    engine = create_engine("sqlite:///{}".format(database_filepath))
    with engine.connect() as connection:
        df = pd.read_sql_table("appen", connection)
        # define the features and output of the model
        X = df["message"]
        Y = df.drop(["message", "id", "original", "genre"], axis=1)
        category_names = list(Y.columns)

        return X, Y, category_names


# Tokenize function
def tokenize(text):
    """Perform tokenization and lemmatization on the input text.

    Args: 
    
    text -> string

    Returns:

    Array of strings containing a tokenized and lemmatizated version of the input string without
    characters that are not alphanumeric.

    """
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower())

    tokens = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()

    clean_tokens = []
    for tok in tokens:
        clean_tok = lemmatizer.lemmatize(tok).lower().strip()
        clean_tokens.append(clean_tok)

    return clean_tokens


def build_model():
    """ Build pipeline that transforms a count matrix to a normalized tf or tf-idf representation
        and use linearSVC to classify the input."""
    pipeline = Pipeline(
        [
            ("vect", CountVectorizer(tokenizer=tokenize)),
            ("tfidf", TfidfTransformer()),
            ("clf", MultiOutputClassifier(LinearSVC(dual=False))),
        ]
    )

    # use grid search to optimize the pipeline parameters
    parameters = {"tfidf__use_idf": (True, False), "clf__estimator__C": [1, 100]}
    cv = GridSearchCV(pipeline, param_grid=parameters)

    return cv


def evaluate_model(model, X_test, Y_test, category_names):
    """evaluates by precision, recall, f1-score using classification_report"""
    y_pred_grid = model.predict(X_test)
    print(
        classification_report(Y_test.values, y_pred_grid, target_names=category_names)
    )


def save_model(model, model_filepath):
    """ Export model as a pickle file"""
    pickle.dump(model, open(model_filepath, "wb"))


def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print("Loading data...\n    DATABASE: {}".format(database_filepath))
        X, Y, category_names = load_data(database_filepath)
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)

        print("Building model...")
        model = build_model()

        print("Training model...")
        model.fit(X_train, Y_train)

        print("Evaluating model...")
        evaluate_model(model, X_test, Y_test, category_names)

        print("Saving model...\n    MODEL: {}".format(model_filepath))
        save_model(model, model_filepath)

        print("Trained model saved!")

    else:
        print(
            "Please provide the filepath of the disaster messages database "
            "as the first argument and the filepath of the pickle file to "
            "save the model to as the second argument. \n\nExample: python "
            "train_classifier.py ../data/DisasterResponse.db classifier.pkl"
        )


if __name__ == "__main__":
    main()
