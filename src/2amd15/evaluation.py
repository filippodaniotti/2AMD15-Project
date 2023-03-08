from typing import List
from matplotlib import pyplot as plt

def plot(t_values: List[str], results: List[int]):
    ax = plt.axes()
    ax.bar(t_values, results, width=0.4)

    ax.set_xlabel("τ values")
    ax.set_ylabel("triples found")
    ax.set_title("Triples found as τ increases")

    for rect, label in zip(ax.patches, results):
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            height + 5, str(label), ha="center", va="bottom"
        )

    plt.savefig('question2.png')
