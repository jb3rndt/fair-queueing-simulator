import matplotlib.pyplot as plt


def plot_boxplots(results, raw_results, trace):
    delays = {
        simulator: data["packet_delays_per_flow"].values()
        for simulator, data in raw_results[trace].items()
    }
    nflows = max(len(delays[simulator]) for simulator in delays.keys())

    labels = [simulator for simulator in results[trace].keys()]

    fig, ax = plt.subplots()
    colors = [
        "lightblue",
        "lightgreen",
        "lightyellow",
        "plum",
        "lightcyan",
        "lightgray",
        "lightpink",
    ][:nflows]

    for n, (simulator, delays) in enumerate(delays.items()):
        bp = ax.boxplot(
            delays,
            positions=range(
                (n * (len(delays) + 1)), (n * (len(delays) + 1)) + len(delays)
            ),
            widths=0.6,
            patch_artist=True,
        )
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)

    # Create a custom legend
    legend_handles = [plt.Line2D([0], [0], color=color, lw=4) for color in colors]
    ax.legend(legend_handles, [f"Flow {i}" for i in range(nflows)], title="Flows")

    ax.set_xticks([2, 8, 14])
    ax.set_xticklabels(labels)

    ax.set_ylabel("Delay in microseconds")

    plt.tight_layout()
    plt.savefig(f"images/boxplot-{trace}.png")


def generate_latex_tables(results):
    indent = "                     "
    with open("tables.txt", "w") as f:
        for trace, data in results.items():
            f.write(f"Trace: {trace}\n")
            f.write("\\begin{tabular}{ll|lllll|}\n")
            f.write(indent + "& & Flow 0 & Flow 1 & Flow 2 & Flow 3 & Flow 4 \\\\\n")
            f.write(indent + "\\hline\n")
            for simulator, results in zip(["GPS", "RR", "DRR"], data.values()):
                # Write throughput per flow, average delay per flow, and standard deviation per flow into a latex table with the flows being the columns and the metrics being the rows
                f.write(("\\multirow{3}{*}{" + simulator + "} ").ljust(21))
                f.write(
                    f"& Throughput & "
                    + " & ".join(
                        [
                            str(round(t, 4))
                            for t in results["throughput_per_flow"].values()
                        ]
                    )
                    + " \\\\\n"
                )
                f.write(
                    f"{indent}& Avg. Delay & "
                    + " & ".join(
                        [
                            str(int(round(t, 0)))
                            for t in results["average_delay_per_flow"].values()
                        ]
                    )
                    + " \\\\\n"
                )
                f.write(
                    f"{indent}& Std. Dev. & "
                    + " & ".join(
                        [
                            str(int(round(t, 0)))
                            for t in results["standard_deviation_per_flow"].values()
                        ]
                    )
                    + " \\\\\n"
                )
                f.write(indent + "\\hline\n")
            f.write("\\end{tabular}\n")
            f.write("\n")
