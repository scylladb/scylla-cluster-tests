"""
Unit tests for the _compare_current_best_results_average method
to ensure it handles missing workload data gracefully.
"""
import pytest
from sdcm.results_analyze import LatencyDuringOperationsPerformanceAnalyzer


class TestCompareCurrentBestResultsAverage:
    """Test suite for hdr_summary_diff calculation."""

    @pytest.fixture
    def analyzer(self):
        """Create a LatencyDuringOperationsPerformanceAnalyzer instance."""
        return LatencyDuringOperationsPerformanceAnalyzer(
            es_index="test_index",
            email_recipients=(),
            logger=None
        )

    def test_compare_with_matching_workloads(self, analyzer):
        """Test that hdr_summary_diff is calculated correctly when workloads match."""
        current_result = {
            '_add_node': {
                'hdr_summary_average': {
                    'WRITE--WRITE-rt': {
                        'percentile_90': 10.0,
                        'percentile_99': 20.0
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {
            '_add_node': {
                '5.1.dev': {
                    'hdr_summary_average': {
                        'WRITE--WRITE-rt': {
                            'percentile_90': 8.0,
                            'percentile_99': 16.0
                        }
                    },
                    'average_time_operation_in_sec': 80
                }
            }
        }

        analyzer._compare_current_best_results_average(current_result, best_result)

        # Verify hdr_summary_diff is populated
        assert 'hdr_summary_diff' in best_result['_add_node']['5.1.dev']
        diff = best_result['_add_node']['5.1.dev']['hdr_summary_diff']
        assert 'WRITE--WRITE-rt' in diff
        assert diff['WRITE--WRITE-rt']['percentile_90'] == 25.0  # (10-8)/8 * 100
        assert diff['WRITE--WRITE-rt']['percentile_99'] == 25.0  # (20-16)/16 * 100

    def test_compare_with_missing_workload_in_current(self, analyzer):
        """Test that missing workload in current_result is handled gracefully."""
        current_result = {
            '_add_node': {
                'hdr_summary_average': {
                    'READ--READ-rt': {  # Different workload
                        'percentile_90': 10.0,
                        'percentile_99': 20.0
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {
            '_add_node': {
                '5.1.dev': {
                    'hdr_summary_average': {
                        'WRITE--WRITE-rt': {  # Workload exists in best but not in current
                            'percentile_90': 8.0,
                            'percentile_99': 16.0
                        }
                    },
                    'average_time_operation_in_sec': 80
                }
            }
        }

        # This should not raise an exception
        analyzer._compare_current_best_results_average(current_result, best_result)

        # Verify hdr_summary_diff exists but is empty or doesn't have the missing workload
        assert 'hdr_summary_diff' in best_result['_add_node']['5.1.dev']
        diff = best_result['_add_node']['5.1.dev']['hdr_summary_diff']
        # The workload should not be in diff since it doesn't exist in current_result
        assert 'WRITE--WRITE-rt' not in diff

    def test_compare_with_missing_nemesis_in_current(self, analyzer):
        """Test that missing nemesis in current_result is handled gracefully."""
        current_result = {
            '_remove_node': {  # Different nemesis
                'hdr_summary_average': {
                    'WRITE--WRITE-rt': {
                        'percentile_90': 10.0,
                        'percentile_99': 20.0
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {
            '_add_node': {  # Nemesis exists in best but not in current
                '5.1.dev': {
                    'hdr_summary_average': {
                        'WRITE--WRITE-rt': {
                            'percentile_90': 8.0,
                            'percentile_99': 16.0
                        }
                    },
                    'average_time_operation_in_sec': 80
                }
            }
        }

        # This should not raise an exception
        analyzer._compare_current_best_results_average(current_result, best_result)

        # Verify hdr_summary_diff is initialized but empty
        assert 'hdr_summary_diff' in best_result['_add_node']['5.1.dev']
        diff = best_result['_add_node']['5.1.dev']['hdr_summary_diff']
        assert len(diff) == 0  # Should be empty since nemesis is missing

    def test_compare_with_empty_best_result(self, analyzer):
        """Test that empty best_result is handled gracefully."""
        current_result = {
            '_add_node': {
                'hdr_summary_average': {
                    'WRITE--WRITE-rt': {
                        'percentile_90': 10.0,
                        'percentile_99': 20.0
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {}

        # This should not raise an exception
        analyzer._compare_current_best_results_average(current_result, best_result)

        # No assertions needed - just verify it doesn't crash

    def test_compare_with_none_percentile_value(self, analyzer):
        """Test that None percentile values are handled gracefully."""
        current_result = {
            '_add_node': {
                'hdr_summary_average': {
                    'WRITE--WRITE-rt': {
                        'percentile_90': None,  # None value
                        'percentile_99': 20.0
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {
            '_add_node': {
                '5.1.dev': {
                    'hdr_summary_average': {
                        'WRITE--WRITE-rt': {
                            'percentile_90': 8.0,
                            'percentile_99': 16.0
                        }
                    },
                    'average_time_operation_in_sec': 80
                }
            }
        }

        # This should not raise an exception
        analyzer._compare_current_best_results_average(current_result, best_result)

        # Verify hdr_summary_diff is populated
        assert 'hdr_summary_diff' in best_result['_add_node']['5.1.dev']
        diff = best_result['_add_node']['5.1.dev']['hdr_summary_diff']
        # The diff for percentile_90 should be "N/A" since current value is None
        assert diff['WRITE--WRITE-rt']['percentile_90'] == "N/A"

    def test_hdr_summary_diff_always_initialized(self, analyzer):
        """Test that hdr_summary_diff is always initialized even on error."""
        current_result = {
            '_add_node': {
                'hdr_summary_average': {
                    'WRITE--WRITE-rt': {
                        'percentile_90': 10.0,
                    }
                },
                'average_time_operation_in_sec': 100
            }
        }

        best_result = {
            '_add_node': {
                '5.1.dev': {
                    'hdr_summary_average': {
                        'WRITE--WRITE-rt': {
                            'percentile_90': 8.0,
                        }
                    },
                    'average_time_operation_in_sec': 80
                }
            }
        }

        analyzer._compare_current_best_results_average(current_result, best_result)

        # Verify hdr_summary_diff exists regardless of success
        assert 'hdr_summary_diff' in best_result['_add_node']['5.1.dev']
