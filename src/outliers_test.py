import pytest

from outliers import detect_outliers


@pytest.fixture
def sample_out_return_data():
    """Vote Count per week Return fixture."""
    return [
        ('2022', '00', 1),
        ('2022', '01', 3),
        ('2022', '02', 3),
        ('2022', '05', 1),
        ('2022', '06', 1),
        ('2022', '08', 1)
    ]


def test_detect_outliers(sample_out_return_data):
    detect_out_return = detect_outliers()
    assert detect_out_return == sample_out_return_data
