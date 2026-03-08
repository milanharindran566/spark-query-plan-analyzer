import re
from graphviz import Digraph
import re

def clean_plan(plan_text):

    lines = plan_text.split("\n")
    cleaned = []

    for line in lines:

        line = line.strip()

        # remove column IDs like #281
        line = re.sub(r"#\d+", "", line)

        # remove long file paths
        line = re.sub(r"Location:.*", "", line)

        # remove plan ids
        line = re.sub(r"\[plan_id=.*?\]", "", line)

        # shorten relation info
        line = re.sub(r"Relation \[.*?\]", "Relation", line)

        cleaned.append(line)

    return "\n".join(cleaned)

def get_color(node):

    if "Exchange" in node:
        return "white"

    if "HashAggregate" in node:
        return "orange"

    if "FileScan" in node:
        return "yellow"

    return "lightblue"

def visualize_plan(plan_text):
    dot = Digraph()
    dot.attr(rankdir="TB")   # top to bottom layout
    nodes = []

    for line in plan_text.split("\n"):
        line = line.strip()
        if line == "":
            continue

        # remove tree symbols
        clean = re.sub(r"[+\-| ]+", "", line)
        
        # extract operator name
        operator = clean.split("(")[0]
        nodes.append(operator)

    # create nodes
    for i, node in enumerate(nodes):
        dot.node(str(i), node, style="filled", fillcolor=get_color(node))

        if i > 0:
            dot.edge(str(i-1), str(i))
    return dot

def analyze_performance(plan):
    warnings = []
    shuffle_count = plan.count("Exchange")

    if shuffle_count > 0:
        warnings.append(f"⚠ Shuffle detected ({shuffle_count} stage)")

    if "HashAggregate" in plan:
        warnings.append("Aggregation detected")

    if "BroadcastHashJoin" in plan:
        warnings.append("Broadcast Join detected")

    if "SortMergeJoin" in plan:
        warnings.append("Sort Merge Join detected (shuffle heavy)")

    if "FileScan" in plan:
        warnings.append("File scan operation")

    return warnings