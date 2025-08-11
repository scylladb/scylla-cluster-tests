# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2025 ScyllaDB

import random
import math
from typing import List, Tuple, Union


def bytes_to_readable(num_bytes):
    """
    Convert bytes to a human-readable format.
    """

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} EB"  # In case it's extremely large


class IsolationTree:
    """
    A single tree in the Isolation Forest, used for anomaly detection.
    """

    def __init__(self, height_limit):
        self.height_limit = height_limit
        self.left = None
        self.right = None
        self.split_value = None
        self.size = 0
        self.is_leaf = False

    def fit(self, data, current_height=0):
        self.size = len(data)

        if current_height >= self.height_limit or len(data) <= 1:
            self.is_leaf = True
            return

        min_val = min(data)
        max_val = max(data)
        if min_val == max_val:
            self.is_leaf = True
            return

        self.split_value = random.uniform(min_val, max_val)
        left_data = [x for x in data if x < self.split_value]
        right_data = [x for x in data if x >= self.split_value]

        self.left = IsolationTree(self.height_limit)
        self.right = IsolationTree(self.height_limit)
        self.left.fit(left_data, current_height + 1)
        self.right.fit(right_data, current_height + 1)

    def path_length(self, x, current_height=0):
        if self.is_leaf or not self.left or not self.right:
            return current_height + self._c(self.size)
        if x < self.split_value:
            return self.left.path_length(x, current_height + 1)
        else:
            return self.right.path_length(x, current_height + 1)

    def _c(self, size):
        if size <= 1:
            return 0
        return 2 * math.log(size - 1) + 0.5772156649  # Euler-Mascheroni constant


class IsolationForest:
    """
    An Isolation Forest implementation for anomaly detection.
    """

    def __init__(self, n_estimators=100, sample_size=64):
        self.n_estimators = n_estimators
        self.sample_size = sample_size
        self.trees = []

    def fit(self, data: List[float]):
        self.trees = []
        for _ in range(self.n_estimators):
            sample = random.choices(data, k=min(self.sample_size, len(data)))
            tree = IsolationTree(height_limit=math.ceil(math.log2(self.sample_size)))
            tree.fit(sample)
            self.trees.append(tree)

    def anomaly_scores(self, data: List[float]) -> List[float]:
        scores = []
        for x in data:
            path_lengths = [tree.path_length(x) for tree in self.trees]
            avg_path_length = sum(path_lengths) / len(path_lengths)
            score = 2 ** (-avg_path_length / self._c(self.sample_size))
            scores.append(score)
        return scores

    def _c(self, size):
        if size <= 1:
            return 0
        return 2 * math.log(size - 1) + 0.5772156649


def detect_isolation_forest_anomalies(
    memory_data: List[Tuple[int, Union[str, int, float]]],
    contamination: float = 0.10,
    filter_score: float = 0.70,
    window_seconds: int = 600,  # 10 minutes
    deviation_threshold: float = 0.20,  # 5%
    ignore_first_minutes: int = 10  # Ignore peaks in the first X minutes
) -> List[Tuple[int, Tuple[int, Union[str, int, float]], str, float]]:
    """
    Pure Python Isolation Forest implementation for anomaly detection, with rolling window deviation filtering.

    :param memory_data: List of tuples containing (timestamp, value) pairs.
    :param contamination: Proportion of outliers in the data.
    :param filter_score: Minimum score to consider a point as an anomaly.
    :param window_seconds: Size of the rolling window in seconds (default 600 = 10min).
    :param deviation_threshold: Minimum deviation from rolling mean to report (as a fraction, default 0.05 = 5%).
    :param ignore_first_minutes: Ignore peaks in the first X minutes from the start timestamp.
    :return: List of detected anomalies with their index, timestamp, value, kind (peak/low), and score.
    """
    if not memory_data:
        raise ValueError("memory_data is empty")

    values = [float(v) for _, v in memory_data]

    forest = IsolationForest(n_estimators=100, sample_size=min(64, len(values)))
    forest.fit(values)

    scores = forest.anomaly_scores(values)
    threshold = sorted(scores, reverse=True)[int(len(scores) * contamination)]

    results = []
    start_timestamp = memory_data[0][0] if memory_data else 0
    ignore_seconds = ignore_first_minutes * 60
    for i, ((timestamp, val), score) in enumerate(zip(memory_data, scores)):
        if ignore_first_minutes > 0 and (timestamp - start_timestamp) < ignore_seconds:
            continue  # Skip anomalies in the first X minutes
        if score >= threshold and score >= filter_score:
            # Rolling window: find points within window_seconds before current timestamp
            window_start = timestamp - window_seconds
            window_vals = [float(v) for (ts, v) in memory_data if window_start <= ts < timestamp]
            if not window_vals:
                # If no previous data, skip deviation check
                continue
            rolling_mean = sum(window_vals) / len(window_vals)
            deviation = abs(float(val) - rolling_mean) / (rolling_mean if rolling_mean != 0 else 1)
            if deviation >= deviation_threshold:
                kind = 'peak' if float(val) > rolling_mean else 'low'
                results.append((i, (timestamp, float(val)), kind, round(score, 3)))

    return results
