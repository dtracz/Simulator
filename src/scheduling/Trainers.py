import numpy as np
import torch
from joblib import Parallel, delayed
from scheduling.VMPlacementPolicies import VMPlacementPolicyAI
from Listeners import AUPMetric


class RandomTrainer:
    def __init__(self, model, score_fun, epoch_size=100, n_bests=20,
                 init_min=-1, init_max=1, n_threads=6):
        self.model = model
        self.score_fun = score_fun
        self._epoch_size = epoch_size
        self._n_bests = n_bests
        self._var_shapes = []
        self._len_theta = 0
        for var in model.getVars():
            self._var_shapes += [var.shape]
            self._len_theta += np.prod(var.shape)
        self._thetas = np.random.uniform(init_min, init_max,
                                         (epoch_size, self._len_theta))
        self._scores = -np.ones(epoch_size)*float('inf')
        self._n_threads = n_threads

    def toVars(self, theta):
        var_sizes = np.prod(self._var_shapes, axis=1)
        splitpoints = np.cumsum(var_sizes)[:-1]
        varList = np.array_split(theta, splitpoints)
        for i, shape in enumerate(self._var_shapes):
            varList[i] = varList[i].reshape(shape)
        return varList

    def scoreThetas(self):
        def score_theta(i):
            varList = self.toVars(self._thetas[i])
            self.model.setVars(varList)
            return self.score_fun(self.model)
        scores = Parallel(n_jobs=self._n_threads)(
                delayed(score_theta)(i) for i in range(self._epoch_size)
        )
        #  scores = [score_theta(i) for i in range(self._epoch_size)]
        self._scores = np.array(scores)

    def getBests(self, n_bests):
        idc = np.argpartition(self._scores, -self._n_bests)[-self._n_bests:]
        bests = self._thetas[idc]
        best_scores = self._scores[idc]
        return bests, best_scores

    def step(self):
        self.scoreThetas()
        bests, best_scores = self.getBests(self._n_bests)
        self._thetas = np.random.normal(bests.mean(axis=0),
                                        bests.std(axis=0),
                                        (self._epoch_size, self._len_theta))
        self._scores = -np.ones(self._epoch_size)*float('inf')
        return best_scores.mean()

    def train(self, n_steps):
        progress_line = []
        for _ in range(n_steps):
            score = self.step()
            progress_line += [score]
        return progress_line



class PolicyGradient(VMPlacementPolicyAI):

    class Reward_F(AUPMetric):
        def __call__(self, jobs):
            rewards = []
            for job in jobs:
                sched, start, f = self._jobs[job]
                cost = self._calculateCost(sched, start, f)
                #  print(job.name, -cost)
                rewards += [-cost]
            return rewards

    def __init__(self, *args, lr=0.01, gamma=0.95, **kwargs):
        super().__init__(*args, **kwargs)
        self._optim = torch.optim.Adam(self._model.parameters(),
                                       lr=lr)
        self._gamma = gamma
        self._reward_f = None
        self._history = [] #[(vm, log(P(action)) )]

    def placeVM(self, vm):
        if self._reward_f is None:
            self._reward_f = self.Reward_F()
        task_info = self._getTaskInfo(vm)
        state_info = [self._getMachineInfo(machine)
                        for machine in self._machines]
        state_info = np.array(state_info)
        state_info = np.expand_dims(state_info, 0)
        task_info = np.expand_dims(task_info, 0)
        #  print(state_info, task_info)
        torch_probs = self._model((state_info, task_info))
        #  print(torch_probs)
        c = torch.distributions.Categorical(torch_probs)
        np_probs = torch_probs.detach().numpy()[0] + 1e-8
        #  checked = set()
        #  print("------------------------------------------------")
        #  print(np_probs)
        #  print(" - - - - - -")
        while True:
            #  action = c.sample()
            action = np.random.choice(np_probs.shape[-1], p=np_probs)
            machine = self._machines[action]
            if machine.isFittable(vm):
                break
            np_probs[action] = 0
            np_probs /= np_probs.sum()
            #  print(np_probs)
            #  print(np_probs.sum())
            #  c = torch.distributions.Categorical(torch_probs)
            #  checked.add(action.item())
            #  print('.', end='')
            #  if len(checked) >= len(self._machines):
            #      print(checked)
            #      raise Exception(f"Non of the known machines"
            #                      f" is suitable for {vm.name}")
        job = vm._jobScheduler._jobQueue[0]
        #  self._history += [(job, c.log_prob(action))]
        self._history += [(job, torch.log(torch_probs[0,action]))]
        scheduler = self._schedulers[machine]
        scheduler.schedule(vm)

    @staticmethod
    def suff_sum(arr, gamma=1):
        res = []
        last = 0
        for elem in arr[::-1]:
            res += [gamma*last + elem]
            last = res[-1]
        return res[::-1]

    def preprocess_rewards(self, rewards):
        rewards = torch.tensor(rewards)
        rewards = (rewards - rewards.mean()) / rewards.std()
        #  rewards = (rewards - rewards.min()) / (rewards.max() - rewards.min())
        #  print(rewards)
        return rewards

    def step(self):
        jobs, log_probs = list(zip(*tuple(self._history)))
        log_probs = torch.stack(log_probs)
        rewards = self._reward_f(jobs)
        rewards = self.suff_sum(rewards, self._gamma)
        rewards = self.preprocess_rewards(rewards)
        #  print(log_probs.shape)
        #  print(rewards.shape)
        loss = -torch.sum(log_probs * rewards)
        self._optim.zero_grad()
        loss.backward()
        #  self._model.float()
        self._optim.step()
        # clean last run
        self._reward_f.unregister()
        self._reward_f = None
        self._history = []
        return loss.item()

