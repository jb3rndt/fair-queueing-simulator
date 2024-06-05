import sys
from typing import Dict, List

from packet import Packet


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