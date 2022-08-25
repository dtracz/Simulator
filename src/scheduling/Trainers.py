import numpy as np
from joblib import Parallel, delayed


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

