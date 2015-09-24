'''
Created on Sep 22, 2015

@author: hustnn
'''
from __future__ import print_function

import os
import subprocess

import pandas as pd
#import numpy as np
from sklearn.tree import DecisionTreeClassifier, export_graphviz

def get_iris_data():
    """Get the iris data, from local csv or pandas repo."""
    if os.path.exists("iris.csv"):
        print("-- iris.csv found locally")
        df = pd.read_csv("iris.csv", index_col=0)
    else:
        print("-- trying to download from github")
        fn = "https://raw.githubusercontent.com/pydata/pandas/" + \
             "master/pandas/tests/data/iris.csv"
        try:
            df = pd.read_csv(fn)
        except:
            exit("-- Unable to download iris.csv")

        with open("iris.csv", 'w') as f:
            print("-- writing to local iris.csv file")
            df.to_csv(f)

    return df


def encode_target(df, target_column):
    """Add column to df with integers for the target.

    Args
    ----
    df -- pandas DataFrame.
    target_column -- column to map to int, producing
                     new Target column.

    Returns
    -------
    df_mod -- modified DataFrame.
    targets -- list of target names.
    """
    df_mod = df.copy()
    targets = df_mod[target_column].unique()
    map_to_int = {name: n for n, name in enumerate(targets)}
    df_mod["Target"] = df_mod[target_column].replace(map_to_int)

    return (df_mod, targets)


def visualize_tree(tree, feature_names):
    """Create tree png using graphviz.

    Args
    ----
    tree -- scikit-learn DecsisionTree.
    feature_names -- list of feature names.
    """
    with open("dt.dot", 'w') as f:
        export_graphviz(tree, out_file=f,
                        feature_names=feature_names)

    command = ["dot", "-Tpng", "dt.dot", "-o", "dt.png"]
    try:
        subprocess.check_call(command)
    except:
        exit("Could not run dot, ie graphviz, to "
             "produce visualization")


if __name__ == '__main__':
    df = get_iris_data()
    
    #print("* df.head()", df.head(), sep="\n", end="\n\n")
    #print("* df.tail()", df.tail(), sep="\n", end="\n\n")
    
    #print("* iris types:", df["Name"].unique(), sep="\n")
    
    df2, targets = encode_target(df, "Name")
    print("* df2.head()", df2.head(), sep="\n", end="\n\n")
    print("* targets", targets, sep="\n", end="\n\n")
    print("* df2.head()", df2[["Target", "Name"]].head(), sep="\n", end="\n\n")
    print("* df2.tail()", df2[["Target", "Name"]].tail(), sep="\n", end="\n\n")
    
    features = list(df2.columns[:4])
    print("* features:", features, sep="\n")
    
    y = df2["Target"]
    X = df2[features]
    dt = DecisionTreeClassifier(min_samples_split=20, random_state=99)
    dt.fit(X, y)
    
    visualize_tree(dt, features)