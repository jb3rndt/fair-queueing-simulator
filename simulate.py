import sys
from pathlib import Path
from typing import Dict, List, Tuple


class Packet:
    def __init__(self, flow, size, time):
        self.flow = flow
        self.size = size
        self.remaining_size = size
        self.time = time

    def __repr__(self):
        return f"Packet(flow={self.flow}, size={self.size}, time={self.time})"


def gps(packet_arrivals: List[Packet], data_rate: int):
    flow_queues: Dict[int, List[Packet]] = {}
    for packet in packet_arrivals:
        if packet.flow not in flow_queues:
            flow_queues[packet.flow] = []
    sent_bits_per_flow = {flow: 0 for flow in flow_queues.keys()}
    total_delay_per_flow = {flow: 0 for flow in flow_queues.keys()}

    time = 0
    next_packet_time = packet_arrivals[0].time

    # If there are no packets at time 0, move time to the next packet time because nothing can be done in the meantime
    time = next_packet_time

    while len(packet_arrivals) > 0:
        # Enqueue arriving packets at the current time and remove them from the arrival list
        while len(packet_arrivals) > 0 and packet_arrivals[0].time == time:
            flow_queues[packet_arrivals[0].flow].append(packet_arrivals.pop(0))

        if len(packet_arrivals) > 0:
            next_packet_time = packet_arrivals[0].time
        else:
            next_packet_time = sys.maxsize

        # Send bits until the next packet arrives or all packets finished
        while time < next_packet_time and any(
            len(queue) > 0 for queue in flow_queues.values()
        ):
            # Send one bit of the first packet in each flow in round-robin fashion
            # Skip the time forward until the next packet arrives or one packet finishes
            skipped_time = min(
                next_packet_time - time,
                min(
                    queue[0].remaining_size
                    for queue in flow_queues.values()
                    if len(queue) > 0
                ),
            )
            time += skipped_time

            for queue in flow_queues.values():
                if len(queue) > 0:
                    if queue[0].remaining_size == skipped_time:
                        print(f"Successfully sent packet {queue[0]}")
                        sent_bits_per_flow[queue[0].flow] += queue[0].size
                        total_delay_per_flow[queue[0].flow] += time - queue[0].time
                        queue.pop(0)
                    else:
                        queue[0].remaining_size -= skipped_time  # TODO: Data rate

        # If all packets finished, move time to the next packet time
        time = next_packet_time

    return time, sent_bits_per_flow, total_delay_per_flow


def main():
    traces_folder = Path("traces")
    for trace_file in traces_folder.iterdir():
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
        # TODO: Duplicate packet_arrivals

        # gps(packet_arrivals, 1)  # 1 bit per time unit (microseconds in this case)
        # round_robin(packet_arrivals)

        total_time, sent_bits_per_flow, total_delay_per_flow = deficit_round_robin(packet_arrivals.copy())
        packets_per_flow = {flow: len([packet for packet in packet_arrivals if packet.flow == flow]) for flow in sent_bits_per_flow.keys()}
        print(f"Total time: {total_time}")
        for flow, sent_bits in sent_bits_per_flow.items():
            print(f"Flow {flow}:")
            print(f"\tThroughput: {sent_bits / total_time} bits per time unit")
            print(f"\tAverage delay: {total_delay_per_flow[flow] / packets_per_flow[flow]} time units")
        print()


def deficit_round_robin(packet_arrivals):
    quantum = 500
    flow_queues: Dict[int, List[Packet]] = {}
    for packet in packet_arrivals:
        if packet.flow not in flow_queues:
            flow_queues[packet.flow] = []
    deficit_counter_per_flow = {flow: 0 for flow in flow_queues.keys()}
    sent_bits_per_flow = {flow: 0 for flow in flow_queues.keys()}
    total_delay_per_flow = {flow: 0 for flow in flow_queues.keys()}

    time = packet_arrivals[0].time
    while len(packet_arrivals) > 0:
        while len(packet_arrivals) > 0 and packet_arrivals[0].time <= time:
            flow_queues[packet_arrivals[0].flow].append(packet_arrivals.pop(0))

        for flow, queue in flow_queues.items():
            if len(queue) > 0:
                # Is the deficit of a flow increased only if it has packets to send?
                deficit_counter_per_flow[flow] += quantum
                # Is a flow allowed to consume all of its built up deficit in one go? (using multiple packets)
                while (
                    len(queue) > 0 and deficit_counter_per_flow[flow] >= queue[0].size
                ):
                    deficit_counter_per_flow[flow] -= queue[0].size
                    # print(f"Flow {flow} sent packet {queue[0]}")
                    time += queue[0].size  # TODO: Data rate
                    sent_bits_per_flow[flow] += queue[0].size
                    total_delay_per_flow[flow] += time - queue[0].time
                    queue.pop(0)

                # While sending the packet, check if any new packets arrived
                while len(packet_arrivals) > 0 and packet_arrivals[0].time <= time:
                    flow_queues[packet_arrivals[0].flow].append(packet_arrivals.pop(0))

            # All flows might be empty, so skip time to the next packet arrival
        if len(packet_arrivals) > 0 and all(
            len(queue) == 0 for queue in flow_queues.values()
        ):
            time = packet_arrivals[0].time

    return time, sent_bits_per_flow, total_delay_per_flow


def round_robin(packet_arrivals):
    flow_queues: Dict[int, List[Packet]] = {}
    for packet in packet_arrivals:
        if packet.flow not in flow_queues:
            flow_queues[packet.flow] = []
    sent_bits_per_flow = {flow: 0 for flow in flow_queues.keys()}
    total_delay_per_flow = {flow: 0 for flow in flow_queues.keys()}

    time = packet_arrivals[0].time
    while len(packet_arrivals) > 0:
        while len(packet_arrivals) > 0 and packet_arrivals[0].time <= time:
            flow_queues[packet_arrivals[0].flow].append(packet_arrivals.pop(0))

        # Send first packet in each flow per round
        for flow, queue in flow_queues.items():
            if len(queue) > 0:
                print(f"Flow {flow} sent packet {queue[0]}")
                sent_bits_per_flow[flow] += queue[0].size
                total_delay_per_flow[flow] += time - queue[0].time
                packet = queue.pop(0)
                time += packet.size  # TODO: Data rate

                # While sending the packet, check if any new packets arrived
                while len(packet_arrivals) > 0 and packet_arrivals[0].time <= time:
                    flow_queues[packet_arrivals[0].flow].append(packet_arrivals.pop(0))

        # All flows might be empty, so skip time to the next packet arrival
        if len(packet_arrivals) > 0 and all(
            len(queue) == 0 for queue in flow_queues.values()
        ):
            time = packet_arrivals[0].time

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

    return time, sent_bits_per_flow, total_delay_per_flow


if __name__ == "__main__":
    main()
