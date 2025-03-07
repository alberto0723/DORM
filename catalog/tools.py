from IPython.display import display
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')  # This sets the backend to plot (default TkAgg does not work)
import hypernetx as hnx


def drop_duplicates(dirty_list):
    unique_elems = []
    [unique_elems.append(elem) for elem in dirty_list if elem not in unique_elems]
    return unique_elems


def combine_tables(patterns_list):
    if len(patterns_list) == 0:
        return [[]]
    else:
        current_pattern = patterns_list.pop(0)
        combinations = []
        for combination in combine_tables(patterns_list):
            for current_table in current_pattern:
                temp = combination + [current_table]
                temp.sort()
                combinations.append(temp)
        return drop_duplicates(combinations)


def df_difference(df1, df2):
    return pd.concat([df1, df2, df2], ignore_index=True).drop_duplicates(keep=False)


def show_textual_hypergraph(h):
    # Textual display
    print("-----------------------------------------------Nodes: ")
    display(h.nodes.dataframe)
    print("-----------------------------------------------Edges: ")
    display(h.edges.dataframe)
    print("------------------------------------------Incidences: ")
    display(h.incidences.dataframe)


def show_graphical_hypergraph(h, show_phantoms):
    # Customize node graphical display
    node_colors = []
    node_labels = {}
    for i in h.nodes.dataframe['misc_properties'].items():
        node_labels[i[0]] = i[0]
        if i[1].get('Kind') == 'Identifier':
            node_colors.append('blue')
        elif i[1].get('Kind') == 'Attribute':
            node_colors.append('green')
        elif i[1].get('Kind') == 'Phantom':
            if show_phantoms:
                node_colors.append('yellow')
            else:
                node_colors.append('white')
                node_labels[i[0]] = ''
        else:
            raise ValueError(f"Undefined representation for node '{i[0]}' of kind '{i[1].get('Kind')}'")
    # Customize edge graphical display
    edge_lines = []
    for i in h.edges.dataframe['misc_properties'].items():
        if i[1].get('Kind') == 'Class':
            edge_lines.append('dotted')
        elif i[1].get('Kind') == 'Relationship':
            edge_lines.append('dashed')
        elif i[1].get('Kind') == 'Struct':
            edge_lines.append('dashdot')
        elif i[1].get('Kind') == 'Set':
            edge_lines.append('solid')
        else:
            raise ValueError(f"Wrong kind of edge {i[1].get('Kind')} for {i[0]}")

    # Graphical display
    fig = plt.figure(figsize=(4, 4))
    hnx.drawing.draw(h,
                     edge_labels_on_edge=True,
                     layout_kwargs={'seed': 666},
                     node_labels=node_labels,
                     nodes_kwargs={'facecolors': node_colors},
                     edges_kwargs={'linestyles': edge_lines, 'edgecolor': 'black'},
                     # 'facecolors': edge_colors}, # This fills the edges, but they are not transparent
                     # edge_labels_kwargs={'color': 'black'} # This does not work
                     )
    plt.show()

