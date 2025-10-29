"""Unit tests for cluster_cloud module."""
from unittest.mock import MagicMock
from sdcm.cluster_cloud import xcloud_super_if_supported


class TestXCloudSuperIfSupportedDecorator:
    """Test suite for xcloud_super_if_supported decorator."""

    def test_decorator_calls_base_method_when_supported(self):
        """Test that decorator calls the base class method when xcloud_connect_supported is True."""

        class BaseNode:
            def test_method(self, value):
                return f"BaseNode.test_method({value})"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self, value):
                pass  # This should never be called

        # Create instance with xcloud support enabled
        node = CloudNode()
        node.xcloud_connect_supported = True
        node.log = MagicMock()

        result = node.test_method("test")

        assert result == "BaseNode.test_method(test)"
        node.log.debug.assert_not_called()

    def test_decorator_handles_inheritance_chain(self):
        """Test that decorator correctly handles inheritance chain without infinite recursion."""

        class BaseNode:
            def wait_ssh_up(self, verbose=True):
                return "BaseNode.wait_ssh_up called"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def wait_ssh_up(self, verbose=True):
                pass  # This should never be called directly

        class CloudVSNode(CloudNode):
            pass  # Doesn't override wait_ssh_up

        # Create CloudVSNode instance with xcloud support enabled
        vs_node = CloudVSNode()
        vs_node.xcloud_connect_supported = True
        vs_node.log = MagicMock()

        # This should NOT cause infinite recursion
        result = vs_node.wait_ssh_up()

        assert result == "BaseNode.wait_ssh_up called"
        vs_node.log.debug.assert_not_called()

    def test_decorator_returns_none_when_not_supported(self):
        """Test that decorator returns None when xcloud_connect_supported is False."""

        class BaseNode:
            def test_method(self):
                return "BaseNode.test_method"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        # Create instance with xcloud support disabled
        node = CloudNode()
        node.xcloud_connect_supported = False
        node.log = MagicMock()

        result = node.test_method()

        assert result is None
        node.log.debug.assert_called_once()
        assert "Skip test_method" in str(node.log.debug.call_args)

    def test_decorator_handles_missing_attribute(self):
        """Test that decorator handles missing xcloud_connect_supported attribute."""

        class BaseNode:
            def test_method(self):
                return "BaseNode.test_method"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        # Create instance without xcloud_connect_supported attribute
        node = CloudNode()
        node.log = MagicMock()

        result = node.test_method()

        # Should treat missing attribute as False
        assert result is None
        node.log.debug.assert_called_once()

    def test_decorator_preserves_method_arguments(self):
        """Test that decorator correctly passes arguments to base method."""

        class BaseNode:
            def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
                return f"BaseNode.test_method({arg1}, {arg2}, {kwarg1}, {kwarg2})"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
                pass

        node = CloudNode()
        node.xcloud_connect_supported = True
        node.log = MagicMock()

        result = node.test_method("a", "b", kwarg1="c", kwarg2="d")

        assert result == "BaseNode.test_method(a, b, c, d)"

    def test_decorator_works_with_multiple_inheritance_levels(self):
        """Test decorator works correctly with multiple inheritance levels."""

        class BaseNode:
            def test_method(self):
                return "BaseNode"

        class MiddleNode(BaseNode):
            pass  # Doesn't override test_method

        class CloudNode(MiddleNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        class CloudVSNode(CloudNode):
            pass

        # Test with CloudVSNode (3 levels deep)
        vs_node = CloudVSNode()
        vs_node.xcloud_connect_supported = True
        vs_node.log = MagicMock()

        result = vs_node.test_method()

        assert result == "BaseNode"
