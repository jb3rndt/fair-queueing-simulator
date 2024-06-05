class Packet:
    def __init__(self, flow, size, time):
        self.flow = flow
        self.size = size
        self.remaining_size = size
        self.time = time

    def __repr__(self):
        return f"Packet(flow={self.flow}, size={self.size}, time={self.time})"
