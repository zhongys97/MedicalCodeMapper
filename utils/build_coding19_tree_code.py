import pandas as pd
import numpy as np
from treelib import Node, Tree
import pickle


def get_node_id_from_description(coding_df, description_str):
    return coding_df[coding_df['meaning'].str.match(description_str)]['node_id'].values[0]

def get_description_from_node_id(coding_df, node_id):
    return coding_df[coding_df['node_id'] == node_id]["meaning"].values[0]

def get_short_description_from_node_id(coding_df, node_id):
    description = coding_df[coding_df['node_id'] == node_id]["meaning"].values[0]
    return description[description.index(" ") + 1:]

if __name__ == "__main__":
    coding_df = pd.read_csv("./../resources/old_coding19.tsv", sep='\t')
    chapter_df = coding_df[coding_df['coding'].str.startswith("Chapter")]
    block_df = coding_df[coding_df['coding'].str.startswith("Block")]
    length_three_df = coding_df[coding_df['coding'].str.len() == 3]
    length_four_df = coding_df[coding_df['coding'].str.len() == 4]
    length_five_df = coding_df[coding_df['coding'].str.len() == 5]
    assert coding_df.shape[0] == chapter_df.shape[0] + block_df.shape[0] + length_three_df.shape[0] + length_four_df.shape[0] + length_five_df.shape[0]

    coding19_tree = Tree()
    coding19_tree.create_node("Root", "root")

    for i in range(chapter_df.shape[0]):
        coding, description, node_id, parent = chapter_df.iloc[i, 0], chapter_df.iloc[i, 1], chapter_df.iloc[i, 2], chapter_df.iloc[i, 3]
        coding19_tree.create_node(coding, node_id, parent="root", data=description)

    for i in range(block_df.shape[0]):
        coding, description, node_id, parent = block_df.iloc[i, 0], block_df.iloc[i, 1], block_df.iloc[i, 2], block_df.iloc[i, 3]
        coding19_tree.create_node(coding, node_id, parent=parent, data=description)

    for i in range(length_three_df.shape[0]):
        coding, description, node_id, parent = length_three_df.iloc[i, 0], length_three_df.iloc[i, 1], length_three_df.iloc[i, 2], length_three_df.iloc[i, 3]
        coding19_tree.create_node(coding, node_id, parent=parent, data=description)

    for i in range(length_four_df.shape[0]):
        coding, description, node_id, parent = length_four_df.iloc[i, 0], length_four_df.iloc[i, 1], length_four_df.iloc[i, 2], length_four_df.iloc[i, 3]
        coding19_tree.create_node(coding, node_id, parent=parent, data=description)

    for i in range(length_five_df.shape[0]):
        coding, description, node_id, parent = length_five_df.iloc[i, 0], length_five_df.iloc[i, 1], length_five_df.iloc[i, 2], length_five_df.iloc[i, 3]
        coding19_tree.create_node(coding, node_id, parent=parent, data=description)

    # children = coding19_tree.is_branch(get_node_id_from_description(coding_df, "A04 Other bacterial intestinal infections"))
    #
    # print([get_description_from_node_id(coding_df, child) for child in children])

    print(coding19_tree.DEPTH, coding19_tree.WIDTH)

    with open("coding19_tree.pickle", "wb") as file:
        pickle.dump(coding19_tree, file)

    print(coding19_tree)


    




