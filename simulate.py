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
        self.packet_delays_per_flow[packet.flow].append(self.time - packet.time)

    def run(self):
        raise NotImplementedError


class GPSSimulator(QueueingSimulator):
    def run(self):
        next_packet_time = self.packet_arrivals[0].time

        while len(self.packet_arrivals) > 0:
            self.enqueue_packets()

            if len(self.packet_arrivals) > 0:
                next_packet_time = self.packet_arrivals[0].time
            else:
                next_packet_time = sys.maxsize

            # Send bits until the next packet arrives or all packets finished
            while self.time < next_packet_time and any(
                len(queue) > 0 for queue in self.flow_queues.values()
            ):
                # Send one bit of the first packet in each flow in round-robin fashion
                # Skip the time forward until the next packet arrives or one packet finishes
                skipped_time = min(
                    next_packet_time - self.time,
                    min(
                        queue[0].remaining_size
                        for queue in self.flow_queues.values()
                        if len(queue) > 0
                    ),
                )
                self.time += skipped_time

                for queue in self.flow_queues.values():
                    if len(queue) > 0:
                        if queue[0].remaining_size == skipped_time:
                            self.finish_packet(queue.pop(0))
                        else:
                            queue[0].remaining_size -= skipped_time  # TODO: Data rate

            # If all packets finished, move time to the next packet time
            self.time = next_packet_time

    def __str__(self):
        return f"GPS"


class RoundRobinSimulator(QueueingSimulator):
    def run(self):
        while len(self.packet_arrivals) > 0:
            self.enqueue_packets()

            # Send first packet in each flow per round
            for flow, queue in self.flow_queues.items():
                if len(queue) > 0:
                    packet = queue.pop(0)
                    self.time += packet.size  # TODO: Data rate
                    self.finish_packet(packet)

                    # While sending the packet, check if any new packets arrived
                    self.enqueue_packets()

            # All flows might be empty, so skip time to the next packet arrival
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

    def __str__(self):
        return f"Round-Robin"


class DeficitRoundRobinSimulator(QueueingSimulator):
    def __init__(self, packet_arrivals: List[Packet], data_rate: int, quantum: int):
        super().__init__(packet_arrivals, data_rate)
        self.quantum = quantum
        self.deficit_counters = {flow: 0 for flow in self.flow_queues.keys()}

    def run(self):
        while len(self.packet_arrivals) > 0:
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
                        self.time += packet.size  # TODO: Data rate
                        self.finish_packet(packet)

                    # While sending the packet, check if any new packets arrived
                    self.enqueue_packets()

            # All flows might be empty, so skip time to the next packet arrival
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

    def __str__(self):
        return f"Deficit Round-Robin"


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

        # gps(packet_arrivals, 1)  # 1 bit per time unit (microseconds in this case)
        # round_robin(packet_arrivals)

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

    # Plot results
    trace = "trace.txt"
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

    ax.set_title(f"Packet delay per flow by simulator for trace \"{trace}\"")
    ax.set_xlabel("Simulators")
    ax.set_ylabel("Delay in microseconds")

    plt.tight_layout()
    plt.show()


def plot_throughputs(results):
    trace = "trace.txt"
    simulators = list(results[trace].keys())
    throughputs = {}
    for simulator, data in results[trace].items():
        for flow, throughput in data["throughput_per_flow"].items():
            if flow not in throughputs:
                throughputs[flow] = []
            throughputs[flow].append(throughput)

    penguin_means = {
        "Bill Depth": (18.35, 18.43, 14.98),
        "Bill Length": (38.79, 48.83, 47.50),
        "Flipper Length": (189.95, 195.82, 217.19),
    }

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


def round_robin(packet_arrivals):
    # while any(len(queue) > 0 for queue in flow_queues.values()):
    #     # Find flow with earliest finishing round
    #     next_packet, min_finishing_round = None, None
    #     for flow, queue in flow_queues.items():
    #         if len(queue) == 0:
    #             continue
    #         packet = queue[0]
    #         if next_packet is None or finishing_round < min_finishing_round:
    #             next_packet = packet
    #             min_finishing_round = finishing_round

    #     print(f"Flow {next_packet.flow} sent packet {next_packet}")
    #     flow_queues[next_packet.flow].pop(0)
    pass


if __name__ == "__main__":
    main()
