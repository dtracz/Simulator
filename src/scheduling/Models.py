import numpy as np


class RandomModel:
    def __init__(self, task_dim, machine_dims, a_space):
        self.a_space = a_space

    def __call__(self, inp):
        task_x, machine_x = inp
        y = np.arange(self.a_space)
        np.random.shuffle(y)
        return y

