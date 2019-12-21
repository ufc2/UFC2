import sys
sys.path.append("..")
import diff_match_patch
import utils

class operation(object):

    def __init__(self, type, pos, data):
        self.type = type
        self.pos = pos
        self.data = data

def infer(v0, v1):
    dmp = diff_match_patch.diff_match_patch()
    diffs = dmp.diff_main(utils.bytes2str(v0), utils.bytes2str(v1))
    dmp.diff_cleanupEfficiency(diffs)
    index = 0
    sequence = []
    op = None
    for diff in diffs:
        data = utils.str2bytes(diff[1])
        if diff[0] == 0:
            index += len(data)
        elif diff[0] == 1:
            if op is not None and op.type == 'd':
                op = operation('i', op.pos, data)
            else:
                op = operation('i', index, data)
            sequence.append(op)
        else:
            op = operation('d', index, data)
            sequence.append(op)
            index += len(data)
    return sequence

def overlap(o1, o2):
    if o1.type == 'i' and o2.type == 'i':
        return o1.pos == o2.pos
    elif o1.type == 'i' and o2.type == 'd':
        return o2.pos <= o1.pos < o2.pos + len(o2.data)
    elif o1.type == 'd' and o2.type == 'i':
        return o1.pos <= o2.pos < o1.pos + len(o1.data)
    else:
        left = max(o1.pos, o2.pos)
        right = min(o1.pos + len(o1.data), o2.pos + len(o2.data))
        return left < right

def detect(seq1, seq2):
    seq_temp = []
    i1 = 0
    i2 = 0
    while i1 < len(seq1) and i2 < len(seq2):
        if seq1[i1].pos < seq2[i2].pos:
            seq_temp.append(seq1[i1])
            i1 +=1
        elif seq1[i1].pos > seq2[i2].pos:
            seq_temp.append(seq2[i2])
            i2 +=1
        else:
            if seq1[i1].type == seq2[i2].type or seq1[i1].type == 'd':
                seq_temp.append(seq1[i1])
                i1 +=1
                seq_temp.append(seq2[i2])
                i2 +=1
            else:
                seq_temp.append(seq2[i2])
                i2 +=1
                seq_temp.append(seq1[i1])
                i1 +=1
    while i1 < len(seq1):
        seq_temp.append(seq1[i1])
        i1 +=1
    while i2 < len(seq2):
        seq_temp.append(seq2[i2])
        i2 +=1

    graph = []
    subgraph = [0]
    for i in range(len(seq_temp)):
        if not i in subgraph:
            graph.append(subgraph)
            subgraph = [i]
        for j in range(i+1, len(seq_temp)):
            if overlap(seq_temp[i], seq_temp[j]):
                if not j in subgraph:
                    subgraph.append(j)
            else:
                break
    graph.append(subgraph)
    return seq_temp, graph

def transform_ii(o1, o2):
    if o1.data == o2.data:
        return None
    o2.pos = o2.pos + len(o1.data)
    return o2

def transform_di(o1, o2):
    o2.pos = o2.pos - len(o1.data)
    if o1.pos > o2.pos:
        o2.pos = o1.pos
    return o2

def transform_dd(o1, o2):
    if o1.data == o2.data:
        return None
    if o1.pos + len(o1.data) >= o2.pos + len(o2.data):
        return None
    common = o1.pos + len(o1.data) - o2.pos
    if common > 0:
        o2.data = o2.data[common:]
        o2.pos = o1.pos
    else:
        o2.pos = o2.pos - len(o1.data)
    return o2

def inclusion(o1, o2):
    if o1.type == 'i' and o2.type == 'i':
        return transform_ii(o1, o2)
    elif o1.type == 'd' and o2.type == 'i':
        return transform_di(o1, o2)
    elif o1.type == 'd' and o2.type == 'd':
        return transform_dd(o1, o2)
    else:
        o2.pos = o2.pos + len(o1.data)
        return o2

def transform_subgraph(subseq, offset):
    seq_s = []
    for op in subseq:
        o2 = op
        for o1 in seq_s:
            o2 = inclusion(o1, o2)
            if o2 is None:
                break
        if not o2 is None:
            seq_s.append(o2)
    effect = 0
    for op in seq_s:
        op.pos = op.pos + offset
        if op.type == 'i':
            effect += len(op.data)
        else:
            effect -= len(op.data)
    return seq_s, effect

def merge(seq, graph):
    seq_result = []
    offset = 0
    for subgraph in graph:
        subseq = seq[subgraph[0]:subgraph[0]+len(subgraph)]
        seq_s, effect = transform_subgraph(subseq, offset)
        seq_result.extend(seq_s)
        offset += effect
    return seq_result

# v0, v1, v2 are bytestrings
def conflict_resolution(v0, v1, v2):
    seq1 = infer(v0, v1)
    seq2 = infer(v0, v2)
    seq, graph = detect(seq1, seq2)
    return merge(seq, graph)

def apply_sequence(v0, seq):
    for op in seq:
        if op.type == 'd':
            v0 = v0[:op.pos] + v0[op.pos + len(op.data):]
        else:
            v0 = v0[:op.pos] + op.data + v0[op.pos :]
    return v0
