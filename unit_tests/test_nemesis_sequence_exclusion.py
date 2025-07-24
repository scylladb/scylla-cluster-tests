"""
Test to verify that NemesisSequence is properly excluded from SisyphusNemesis selection.
This test ensures that the disrupt_run_unique_sequence method is not available for 
random selection by SisyphusNemesis.
"""

import pytest
from sdcm.nemesis import Nemesis, NemesisFlags, COMPLEX_NEMESIS, NemesisSequence
from sdcm.nemesis_registry import NemesisRegistry


def test_nemesis_sequence_in_complex_nemesis():
    """Test that NemesisSequence is included in COMPLEX_NEMESIS list."""
    assert NemesisSequence in COMPLEX_NEMESIS, \
        "NemesisSequence should be included in COMPLEX_NEMESIS to prevent it from being picked by SisyphusNemesis"


def test_nemesis_sequence_excluded_from_registry():
    """Test that NemesisSequence is excluded from the nemesis registry."""
    registry = NemesisRegistry(base_class=Nemesis, flag_class=NemesisFlags, excluded_list=COMPLEX_NEMESIS)
    
    # Get all subclasses that are not excluded
    available_subclasses = registry.filter_subclasses(registry.get_subclasses())
    
    # Verify that NemesisSequence is not in the available subclasses
    assert NemesisSequence not in available_subclasses, \
        "NemesisSequence should be excluded from available nemesis subclasses"


def test_disrupt_run_unique_sequence_not_available():
    """Test that disrupt_run_unique_sequence method is not available for selection."""
    registry = NemesisRegistry(base_class=Nemesis, flag_class=NemesisFlags, excluded_list=COMPLEX_NEMESIS)
    
    # Get all available disrupt methods
    available_methods = registry.get_disrupt_methods()
    available_method_names = [method.__name__ for method in available_methods]
    
    # Verify that disrupt_run_unique_sequence is not in the available methods
    assert "disrupt_run_unique_sequence" not in available_method_names, \
        "disrupt_run_unique_sequence should not be available for selection by SisyphusNemesis"


def test_other_complex_nemeses_also_excluded():
    """Test that other complex nemeses are also properly excluded."""
    registry = NemesisRegistry(base_class=Nemesis, flag_class=NemesisFlags, excluded_list=COMPLEX_NEMESIS)
    
    # Get all subclasses that are not excluded
    available_subclasses = registry.filter_subclasses(registry.get_subclasses())
    
    # Verify that all items in COMPLEX_NEMESIS are excluded
    for complex_nemesis in COMPLEX_NEMESIS:
        assert complex_nemesis not in available_subclasses, \
            f"{complex_nemesis.__name__} should be excluded from available nemesis subclasses"