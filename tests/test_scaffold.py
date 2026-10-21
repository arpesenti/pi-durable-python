from pi_durable import __version__


def test_version_tracks_the_upstream_baseline() -> None:
    assert __version__ == "1.3.0"
