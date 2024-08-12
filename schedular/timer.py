class Timer:
    def __init__(self) -> None:
        self.global_time = 0

    def step(self):
        self.global_time += 1


TIMER = Timer()
