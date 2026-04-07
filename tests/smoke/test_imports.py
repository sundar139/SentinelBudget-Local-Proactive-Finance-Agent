def test_package_imports() -> None:
    import sentinelbudget  # noqa: F401
    import sentinelbudget.config  # noqa: F401
    import sentinelbudget.db.engine  # noqa: F401
    import sentinelbudget.db.healthcheck  # noqa: F401
    import sentinelbudget.demo.bootstrap  # noqa: F401
    import sentinelbudget.logging  # noqa: F401
    import sentinelbudget.preflight  # noqa: F401
