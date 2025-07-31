import pytest
from sdcm.utils.validators.anomalies_detection import detect_isolation_forest_anomalies


def test_empty_memory_data_raises():
    """Test that passing an empty list raises a ValueError."""
    with pytest.raises(ValueError):
        detect_isolation_forest_anomalies([])


def test_detects_anomalies():
    """Test that clear outliers in the data are detected as anomalies."""
    # Create a dataset with clear outliers
    memory_data = [
        (1750323399.385, "8519680"),
        (1750323419.385, "8519680"),
        (1750323439.385, "8519680"),
        (1750323459.385, "8519680"),
        (1750323479.385, "8519680"),
        (1750323499.385, "8519681"),
        (1750323519.385, "8519680"),
        (1750323539.385, "9519680"),
        (1750323559.385, "8519680"),
        (1750323579.385, "8519680"),
    ]
    anomalies = detect_isolation_forest_anomalies(memory_data, contamination=0.15, filter_score=0.5)
    # Should detect both the high and low outliers
    anomaly_indices = [a[0] for a in anomalies]
    assert 7 in anomaly_indices
    # Check kind
    kinds = {a[0]: a[2] for a in anomalies}
    assert kinds[7] == 'peak'


def test_no_anomalies():
    """Test that no anomalies are detected in a uniform dataset."""
    memory_data = [(i, 10) for i in range(20)]
    anomalies = detect_isolation_forest_anomalies(memory_data, contamination=0.1, filter_score=0.99)
    assert anomalies == []


def test_anomalies_with_gradual_and_sharp_changes():
    """Test that only sharp deviations are detected as anomalies with a rolling window."""
    # Simulate 30 minutes of data, 1 point per minute, mostly stable, with two sharp spikes
    base = 10000
    memory_data = []
    for i in range(30):
        ts = 1750323600 + i * 60  # 1 minute apart
        val = base
        if i == 10:
            val = int(base * 1.20)  # 20% spike (increase magnitude)
        if i == 20:
            val = int(base * 0.80)  # 20% drop (increase magnitude)
        memory_data.append((ts, str(val)))
    # Lower deviation threshold to 0.04 for more sensitivity
    anomalies = detect_isolation_forest_anomalies(
        memory_data, window_seconds=600, deviation_threshold=0.04, filter_score=0.5, contamination=0.2)
    assert len(anomalies) >= 1
    assert any(a[1][0] == memory_data[10][0] for a in anomalies)
    assert any(a[1][0] == memory_data[20][0] for a in anomalies)


def test_anomalies_with_high_deviation_threshold():
    """Test that a higher deviation threshold suppresses smaller anomalies."""
    base = 10000
    memory_data = []
    for i in range(15):
        ts = 1750327200 + i * 60
        val = base
        if i == 7:
            val = int(base * 1.06)  # 6% spike
        memory_data.append((ts, str(val)))
    # With 10% threshold, the 6% spike should not be detected
    anomalies = detect_isolation_forest_anomalies(
        memory_data, window_seconds=600, deviation_threshold=0.10, filter_score=0.5, contamination=0.2)
    assert len(anomalies) == 0
    # With 5% threshold, the 6% spike should be detected
    anomalies = detect_isolation_forest_anomalies(
        memory_data, window_seconds=600, deviation_threshold=0.05, filter_score=0.5, contamination=0.2)
    assert any(a[1][0] == memory_data[7][0] for a in anomalies)


def test_anomalies_with_small_time_window():
    """Test that a small time window only considers recent points for deviation."""
    base = 10000
    memory_data = []
    for i in range(20):
        ts = 1750330000 + i * 60
        val = base
        if i == 5:
            val = int(base * 1.20)  # 20% spike
        memory_data.append((ts, str(val)))
    # Lower filter_score and contamination for more sensitivity
    anomalies = detect_isolation_forest_anomalies(
        memory_data, window_seconds=60, deviation_threshold=0.10, filter_score=0.5, contamination=0.2)
    assert any(a[1][0] == memory_data[5][0] for a in anomalies)
