import cdc
import copy

class ocdc(object):

    def __init__(self, ops, boundaries):
        self.ops = ops
        self.boundaries = copy.copy(boundaries)
        self.count = len(self.boundaries)

    def mark(self):
        self.boundaries.reverse()
        for op in self.ops:
            if op.type == 'd':
                i = 0
                while i < self.count:
                    if self.boundaries[i] == -1:
                        pass
                    elif self.boundaries[i] > op.pos:
                        if self.boundaries[i] - cdc.WINSIZE >= op.pos + op.data:
                            self.boundaries[i] -= op.data
                        else:
                            del self.boundaries[i]
                            self.count -= 1
                            i -= 1
                    else:
                        break
                    i += 1
            else:
                i = 0
                while i < self.count:
                    if self.boundaries[i] == -1:
                        pass
                    elif self.boundaries[i] > op.pos:
                        if self.boundaries[i] - cdc.WINSIZE >= op.pos:
                            self.boundaries[i] += len(op.data)
                        else:
                            self.boundaries[i] = -1
                            break;
                    else:
                        self.boundaries.insert(i, -1)
                        self.count += 1
                        break
                    i += 1
        self.boundaries.reverse()

    def recalculate(self, data):
        flag = False
        start = 0
        end = 1
        while start < len(self.boundaries):
            while end < len(self.boundaries) and self.boundaries[end] == -1:
                flag = True
                del self.boundaries[end]
            if end == len(self.boundaries):
                if len(data) - self.boundaries[start] > cdc.MAXSIZE:
                    flag = True
            else:
                if self.boundaries[end] - self.boundaries[start] > cdc.MAXSIZE:
                    flag = True
            if flag:
                if end == len(self.boundaries):
                    sub_boundaries = cdc.cdc(data[self.boundaries[start]:])
                else:
                    sub_boundaries = cdc.cdc(data[self.boundaries[start]:self.boundaries[end]])
                del sub_boundaries[0]
                for boundary in sub_boundaries:
                    self.boundaries.insert(end, boundary + self.boundaries[start])
                    end += 1
            start = end
            end = start + 1
            flag = False
