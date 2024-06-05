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
        self.time = packet_arrivals[0].time

        # Initialize metrics
        self.sent_bits_per_flow = {flow: 0 for flow in self.flow_queues.keys()}
        self.packet_delays_per_flow = {flow: [] for flow in self.flow_queues.keys()}

    def enqueue_packets(self):
        # Enqueue arriving packets to the corresponding flow queues
        while (
            len(self.packet_arrivals) > 0 and self.packet_arrivals[0].time <= self.time
        ):
            packet = self.packet_arrivals.pop(0)
            self.flow_queues[packet.flow].append(packet)

    def update_metrics(self, packet: Packet):
        self.sent_bits_per_flow[packet.flow] += packet.size
        self.packet_delays_per_flow[packet.flow].append(
            self.time - packet.time - (packet.size / self.data_rate)
        )

    def run(self):
        raise NotImplementedError


class GPSSimulator(QueueingSimulator):
    def run(self):
        # Serve packets until no more packets will arrive and all queues are empty
        while len(self.packet_arrivals) > 0 or any(
            len(queue) > 0 for queue in self.flow_queues.values()
        ):
            self.enqueue_packets()

            # Send one bit of the first packet in each flow in round-robin fashion
            # Skip the time forward until the next packet arrives or one packet finishes
            number_of_active_flows = len(
                [1 for queue in self.flow_queues.values() if len(queue) > 0]
            )
            time_until_next_packet = (
                self.packet_arrivals[0].time - self.time
                if len(self.packet_arrivals) > 0
                else sys.maxsize
            )
            fully_skippable_rounds = time_until_next_packet // number_of_active_flows
            # Find the minimum remaining size of the first packet in each flow
            # to determine how many rounds can be skipped. The size is reduced by 1,
            # so the packet will finish in the next round (which will not be squashed then).
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
                    # Always check for new packets
                    self.enqueue_packets()

                    if len(queue) > 0:
                        self.time += 1 / self.data_rate
                        queue[0].remaining_size -= 1
                        if queue[0].remaining_size == 0:
                            self.update_metrics(queue.pop(0))
            else:
                self.time += (
                    skippable_rounds / self.data_rate
                ) * number_of_active_flows
                for queue in self.flow_queues.values():
                    if len(queue) > 0:
                        queue[0].remaining_size -= skippable_rounds

            # If all packets finished, move time to the next arriving packet
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

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
                    self.update_metrics(packet)

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
                    self.deficit_counters[flow] += self.quantum
                    while (
                        len(queue) > 0 and self.deficit_counters[flow] >= queue[0].size
                    ):
                        packet = queue.pop(0)
                        self.deficit_counters[flow] -= packet.size
                        self.time += packet.size / self.data_rate
                        self.update_metrics(packet)

                    # While sending the packet, check if any new packets arrived
                    self.enqueue_packets()

                    # Reset deficit counter if the queue is empty
                    if len(queue) == 0:
                        self.deficit_counters[flow] = 0

            # All flows might be empty, so skip time to the next packet arrival
            if len(self.packet_arrivals) > 0 and all(
                len(queue) == 0 for queue in self.flow_queues.values()
            ):
                self.time = self.packet_arrivals[0].time

    def __str__(self):
        return f"Deficit round robin (DRR)"
