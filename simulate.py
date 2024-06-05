import json
import sys
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np


class Packet:
    def __init__(self, flow, size, time):
        self.flow = flow
        self.size = size
        self.remaining_size = size
        self.time = time

    def __repr__(self):
        return f"Packet(flow={self.flow}, size={self.size}, time={self.time})"


class QueueingSimulator:
    def __init__(self, packet_arrivals: List[Packet], data_rate: int):
        self.packet_arrivals = packet_arrivals
        self.data_rate = data_rate
        self.flow_queues: Dict[int, List[Packet]] = {}
        for packet in packet_arrivals:
            if packet.flow not in self.flow_queues:
                self.flow_queues[packet.flow] = []
        self.sent_bits_per_flow = {flow: 0 for flow in self.flow_queues.keys()}
        self.packet_delays_per_flow = {flow: [] for flow in self.flow_queues.keys()}
        self.time = packet_arrivals[0].time

    def enqueue_packets(self):
        # Enqueue arriving packets at the current time and remove them from the arrival list
        while (
            len(self.packet_arrivals) > 0 and self.packet_arrivals[0].time <= self.time
        ):
            packet = self.packet_arrivals.pop(0)
            self.flow_queues[packet.flow].append(packet)

    def finish_packet(self, packet: Packet):
        self.sent_bits_per_flow[packet.flow] += packet.size
        self.packet_delays_per_flow[packet.flow].append(
            self.time - packet.time - (packet.size / self.data_rate)
        )

    def run(self):
        raise NotImplementedError


class GPSSimulator(QueueingSimulator):
    def run(self):
        next_packet_time = self.packet_arrivals[0].time

        while len(self.packet_arrivals) > 0 or any(
            len(queue) > 0 for queue in self.flow_queues.values()
        ):
            self.enqueue_packets()

            next_packet_time = (
                self.packet_arrivals[0].time if len(self.packet_arrivals) > 0 else None
            )

            # Send bits until the next packet arrives or all packets finished
            while (next_packet_time is None or self.time < next_packet_time) and any(
                len(queue) > 0 for queue in self.flow_queues.values()
            ):
                # Send one bit of the first packet in each flow in round-robin fashion
                # Skip the time forward until the next packet arrives or one packet finishes
                number_of_active_flows = len(
                    [1 for queue in self.flow_queues.values() if len(queue) > 0]
                )
                time_until_next_packet = (next_packet_time or sys.maxsize) - self.time
                fully_skippable_rounds = (
                    time_until_next_packet // number_of_active_flows
                )
                skippable_rounds_until_packet_finishes = min(
                    max(queue[0].remaining_size - 1, 0)
                    for queue in self.flow_queues.values()
                    if len(queue) > 0
                )
                skippable_rounds = min(
                    fully_skippable_rounds, skippable_rounds_until_packet_finishes
                )
                if skippable_rounds == 0:
                    for queue in self.flow_queues.values():
                        if time_until_next_packet == 0:
                            break
                        if len(queue) > 0:
                            self.time += 1 / self.data_rate
                            time_until_next_packet -= 1 / self.data_rate
                            queue[0].remaining_size -= 1
                            if queue[0].remaining_size == 0:
                                self.finish_packet(queue.pop(0))
                else:
                    self.time += (
                        skippable_rounds / self.data_rate
                    ) * number_of_active_flows
                    for queue in self.flow_queues.values():
                        if len(queue) > 0:
                            queue[0].remaining_size -= skippable_rounds
                            if queue[0].remaining_size == 0:
                                self.finish_packet(queue.pop(0))

            # If all packets finished, move time to the next packet time
            if next_packet_time is not None and next_packet_time > self.time:
                self.time = next_packet_time

    def __str__(self):
        return f"GPS"


class RoundRobinSimulator(QueueingSimulator):
    def run(self):
        while len(self.packet_arrivals) > 0 or any(
            len(queue) > 0 for queue in self.flow_queues.values()
        ):
            self.enqueue_packets()

            # Send first packet in each flow per round
            for flow, queue in self.flow_queues.items():
                if len(queue) > 0:
                    packet = queue.pop(0)
                    self.time += packet.size / self.data_rate
                    self.finish_packet(packet)

                    # While sending the packet, check if any new packets arrived
                    self.enqueue_packets()

            # All flows might be empty, so skip time to the next packet arrival
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

    def __str__(self):
        return f"Round robin (RR)"


class DeficitRoundRobinSimulator(QueueingSimulator):
    def __init__(self, packet_arrivals: List[Packet], data_rate: int, quantum: int):
        super().__init__(packet_arrivals, data_rate)
        self.quantum = quantum
        self.deficit_counters = {flow: 0 for flow in self.flow_queues.keys()}

    def run(self):
        while len(self.packet_arrivals) > 0 or any(
            len(queue) > 0 for queue in self.flow_queues.values()
        ):
            self.enqueue_packets()

            for flow, queue in self.flow_queues.items():
                if len(queue) > 0:
                    # Is the deficit of a flow increased only if it has packets to send?
                    self.deficit_counters[flow] += self.quantum
                    # Is a flow allowed to consume all of its built up deficit in one go? (using multiple packets)
                    while (
                        len(queue) > 0 and self.deficit_counters[flow] >= queue[0].size
                    ):
                        packet = queue.pop(0)
                        self.deficit_counters[flow] -= packet.size
                        self.time += packet.size / self.data_rate
                        self.finish_packet(packet)

                    # While sending the packet, check if any new packets arrived
                    self.enqueue_packets()

            # All flows might be empty, so skip time to the next packet arrival
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

    def __str__(self):
        return f"Deficit round robin (DRR)"


def main():
    results = {}
    raw_results = {}
    traces_folder = Path("traces")
    for trace_file in traces_folder.iterdir():
        results[trace_file.name] = {}
        raw_results[trace_file.name] = {}
        packet_arrivals = []
        with open(trace_file, "r") as f:
            for line in f:
                if (
                    line[0] == "#"
                    or line[0] == " "
                    or line[0] == "\n"
                    or "\t" not in line
                ):
                    continue
                flow, size, time = line.split()
                packet_arrivals.append(Packet(int(flow), float(size), int(time)))
        packet_arrivals.sort(key=lambda x: x.time)

        simulators: List[QueueingSimulator] = [
            GPSSimulator(packet_arrivals.copy(), 1),
            RoundRobinSimulator(packet_arrivals.copy(), 1),
            DeficitRoundRobinSimulator(packet_arrivals.copy(), 1, 500),
        ]

        for simulator in simulators:
            simulator.run()

            raw_results[trace_file.name][str(simulator)] = {
                "packet_delays_per_flow": simulator.packet_delays_per_flow,
            }
            results[trace_file.name][str(simulator)] = {
                "time": simulator.time,
                "sent_bits_per_flow": simulator.sent_bits_per_flow,
                "throughput_per_flow": {
                    flow: sent_bits / simulator.time
                    for flow, sent_bits in simulator.sent_bits_per_flow.items()
                },
                "average_delay_per_flow": {
                    flow: sum(delays) / len(delays)
                    for flow, delays in simulator.packet_delays_per_flow.items()
                },
                "standard_deviation_per_flow": {
                    flow: np.std(delays)
                    for flow, delays in simulator.packet_delays_per_flow.items()
                },
            }

    with open("results.json", "w") as f:
        f.write(json.dumps(results, indent=4))

    generate_latex_tables(results)

    # Plot results
    for trace in ["trace.txt", "trace2.txt", "trace3.txt"]:
        plot_boxplots(results, raw_results, trace)

def generate_latex_tables(results):
    indent = "                     "
    with open("tables.txt", "w") as f:
        for trace, data in results.items():
            f.write(f"Trace: {trace}\n")
            f.write("\\begin{tabular}{ll|lllll|}\n")
            f.write(indent+"& & Flow 0 & Flow 1 & Flow 2 & Flow 3 & Flow 4 \\\\\n")
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
                f.write(indent+"\\hline\n")
            f.write("\\end{tabular}\n")
            f.write("\n")


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
    plt.savefig(f"boxplot-{trace}.png")


def plot_throughputs(results):
    trace = "trace.txt"
    simulators = list(results[trace].keys())
    throughputs = {}
    for simulator, data in results[trace].items():
        for flow, throughput in data["throughput_per_flow"].items():
            if flow not in throughputs:
                throughputs[flow] = []
            throughputs[flow].append(throughput)

    x = np.arange(len(simulators))  # the label locations
    width = 1 / len(throughputs.keys())  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(layout="constrained")

    for flow, throughput_per_simulator in throughputs.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, throughput_per_simulator, width, label=flow)
        ax.bar_label(rects, padding=3)
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel("Length (mm)")
    ax.set_title("Penguin attributes by species")
    ax.set_xticks(x + width, simulators)
    ax.legend(loc="upper left", ncols=3)
    ax.set_ylim(0, 0.25)

    plt.show()

if __name__ == "__main__":
    main()
