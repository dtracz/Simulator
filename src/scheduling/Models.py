import numpy as np
import torch


class RandomModel:
    def __init__(self, task_dim, machine_dims, a_space):
        self.a_space = a_space

    def __call__(self, inp):
        task_x, machine_x = inp
        y = np.arange(self.a_space)
        np.random.shuffle(y)
        return y



class Model_v0_np:
    def __init__(self, machines_dims, task_dim, a_space):
        super().__init__()
        mdy, mdx = machines_dims
        self.fc0 = np.random.uniform(0,1, (mdx+task_dim, 32))
        self.fc1 = np.random.uniform(0,1, (32+task_dim, 1))
        self.leakyRelU_param = 0.01

    def getVars(self):
        return [self.fc0, self.fc1]

    def setVars(self, varList):
        assert len(varList) == 2
        assert varList[0].shape == self.fc0.shape
        self.fc0 = varList[0]
        assert varList[1].shape == self.fc1.shape
        self.fc1 = varList[1]

    @staticmethod
    def prepare(inp):
        x, cat = inp
        cat = np.expand_dims(cat, 1)
        cat = np.concatenate(tuple(x.shape[1]*[cat]), 1)
        return x, cat

    def leakyReLU(self, x):
        x = x+0
        x[x<0] *= self.leakyRelU_param
        return x

    @staticmethod
    def softmax(x):
        x = x - x.max(axis=-1)[...,None]
        e_x = np.exp(x)
        y = e_x / e_x.sum(axis=-1)[...,None]
        return y

    def __call__(self, inp):
        x, cat = self.prepare(inp)
        x = np.concatenate((x, cat), 2)
        x = x @ self.fc0
        x = self.leakyReLU(x)
        x = np.concatenate((x, cat), 2)
        x = x @ self.fc1
        x = x[...,-1]
        x = self.softmax(x)
        return x



class Model_v0_torch(torch.nn.Module):
    def __init__(self, machines_dims, task_dim, a_space):
        super().__init__()
        mdy, mdx = machines_dims
        self.conv0 = torch.nn.Conv1d(mdx+task_dim, 32, 1, bias = False)
        self.activ0 = torch.nn.LeakyReLU()
        self.conv1 = torch.nn.Conv1d(32+task_dim, 1, 1, bias = False)
        self.flat = torch.nn.Flatten()
        self.sm = torch.nn.Softmax(dim=-1)

    @staticmethod
    def prepare(inp):
        x, cat = inp
        x = torch.Tensor(x).transpose(1,2) # NLC to NCL
        cat = np.expand_dims(cat, 1).transpose(0,2,1)
        task_dim = cat.shape[1]
        cat = torch.Tensor(cat).expand(-1,task_dim,x.shape[-1])
        return x, cat

    def forward(self, inp):
        x, cat = self.prepare(inp)
        x = torch.cat((x, cat), 1)
        x = self.conv0(x)
        x = self.activ0(x)
        x = torch.cat((x, cat), 1)
        x = self.conv1(x)
        x = self.flat(x)
        x = self.sm(x)
        return x

