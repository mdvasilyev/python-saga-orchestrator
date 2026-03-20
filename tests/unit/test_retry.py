from datetime import timedelta

from src.domain.models import ExponentialRetry, FixedRetry, NoRetry


def test_no_retry_has_no_attempts() -> None:
    policy = NoRetry()
    assert policy.next_delay(1) is None


def test_fixed_retry_returns_constant_delay() -> None:
    policy = FixedRetry(max_attempts=3, delay=timedelta(seconds=5))
    assert policy.next_delay(1) == timedelta(seconds=5)
    assert policy.next_delay(2) == timedelta(seconds=5)
    assert policy.next_delay(3) == timedelta(seconds=5)
    assert policy.next_delay(4) is None


def test_exponential_retry_grows_with_attempts() -> None:
    policy = ExponentialRetry(
        max_attempts=4, base_delay=timedelta(seconds=2), multiplier=2.0
    )
    assert policy.next_delay(1) == timedelta(seconds=2)
    assert policy.next_delay(2) == timedelta(seconds=4)
    assert policy.next_delay(3) == timedelta(seconds=8)
    assert policy.next_delay(4) == timedelta(seconds=16)
    assert policy.next_delay(5) is None


def test_exponential_retry_respects_max_delay_cap() -> None:
    policy = ExponentialRetry(
        max_attempts=5,
        base_delay=timedelta(seconds=3),
        multiplier=3.0,
        max_delay=timedelta(seconds=20),
    )
    assert policy.next_delay(1) == timedelta(seconds=3)
    assert policy.next_delay(2) == timedelta(seconds=9)
    assert policy.next_delay(3) == timedelta(seconds=20)
    assert policy.next_delay(4) == timedelta(seconds=20)
