import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--test-path",
        action="store",
        default=None,
        help="Path to the test data file."
    )
    parser.addoption(
        "--control-path",
        action="store",
        default=None,
        help="Path to the control data file."
    )

@pytest.fixture
def control_path(request):
    return request.config.getoption("--control-path")

@pytest.fixture
def test_path(request):
    return request.config.getoption("--test-path")
