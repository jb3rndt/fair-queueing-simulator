import json
from pathlib import Path
from typing import List

import numpy as np

from packet import Packet
from plotting import generate_latex_tables, plot_boxplots
from simulator import (
    DeficitRoundRobinSimulator,
    GPSSimulator,
    QueueingSimulator,
    RoundRobinSimulator,
)


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


if __name__ == "__main__":
    main()
