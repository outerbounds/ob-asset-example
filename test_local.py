#!/usr/bin/env python
"""
Tests for asset branch resolution logic.

Validates resolve_scope() behavior across deployment contexts.
Run with: python test_local.py
"""

import sys

from obproject.projectbase import resolve_scope


def test_resolve_scope():
    """Test the branch resolution logic for all deployment scenarios."""
    print("=" * 60)
    print("Testing resolve_scope() - Asset Branch Resolution")
    print("=" * 60)
    print()

    tests_passed = 0
    tests_failed = 0

    def run_test(name, project_config, project_spec, expected_branch, description=""):
        nonlocal tests_passed, tests_failed
        print(f"Test: {name}")
        print("-" * 40)
        if description:
            print(f"  Scenario: {description}")
        project, read_branch = resolve_scope(project_config, project_spec)

        if read_branch == expected_branch:
            print(f"  [PASS] read_branch = {repr(read_branch)}")
            tests_passed += 1
        else:
            print(f"  [FAIL] read_branch = {repr(read_branch)} (expected {repr(expected_branch)})")
            tests_failed += 1
        print()

    # === PRODUCTION DEPLOYMENTS ===
    print("=" * 60)
    print("PRODUCTION DEPLOYMENTS (read/write same branch)")
    print("=" * 60)
    print()

    run_test(
        "Production deployment",
        project_config={"project": "my_project"},
        project_spec={"branch": "main", "spec": {"metaflow_branch": "prod"}},
        expected_branch="prod",
        description="Deployed with --production flag"
    )

    run_test(
        "Production deployment with [dev-assets] (ignored)",
        project_config={"project": "my_project", "dev-assets": {"branch": "main"}},
        project_spec={"branch": "main", "spec": {"metaflow_branch": "prod"}},
        expected_branch="prod",
        description="[dev-assets] is ignored for prod deployments"
    )

    run_test(
        "Production variant (prod.v2)",
        project_config={"project": "my_project"},
        project_spec={"branch": "main", "spec": {"metaflow_branch": "prod.v2"}},
        expected_branch="prod.v2",
        description="Production with version suffix"
    )

    # === USER/TEST DEPLOYMENTS ===
    print("=" * 60)
    print("USER/TEST DEPLOYMENTS")
    print("=" * 60)
    print()

    run_test(
        "Test deployment without [dev-assets]",
        project_config={"project": "my_project"},
        project_spec={"branch": "feature", "spec": {"metaflow_branch": "test.feature"}},
        expected_branch="test.feature",
        description="Read/write same test branch"
    )

    run_test(
        "Test deployment WITH [dev-assets]",
        project_config={"project": "my_project", "dev-assets": {"branch": "prod"}},
        project_spec={"branch": "feature", "spec": {"metaflow_branch": "test.feature"}},
        expected_branch="prod",
        description="Write to test.feature, READ from prod"
    )

    run_test(
        "User deployment without [dev-assets]",
        project_config={"project": "my_project"},
        project_spec={"branch": "main", "spec": {"metaflow_branch": "user.alice"}},
        expected_branch="user.alice",
        description="Read/write same user branch"
    )

    run_test(
        "User deployment WITH [dev-assets]",
        project_config={"project": "my_project", "dev-assets": {"branch": "prod"}},
        project_spec={"branch": "main", "spec": {"metaflow_branch": "user.alice"}},
        expected_branch="prod",
        description="Write to user.alice, READ from prod"
    )

    # === LOCAL DEVELOPMENT ===
    print("=" * 60)
    print("LOCAL DEVELOPMENT (no project_spec)")
    print("=" * 60)
    print()

    run_test(
        "Local dev without [dev-assets]",
        project_config={"project": "my_project"},
        project_spec=None,
        expected_branch=None,
        description="Read/write same branch (None = use write branch)"
    )

    run_test(
        "Local dev WITH [dev-assets]",
        project_config={"project": "my_project", "dev-assets": {"branch": "prod"}},
        project_spec=None,
        expected_branch="prod",
        description="Write to user branch, READ from prod"
    )

    # === EDGE CASES ===
    print("=" * 60)
    print("EDGE CASES")
    print("=" * 60)
    print()

    run_test(
        "Legacy: metaflow_branch not set",
        project_config={"project": "legacy_project"},
        project_spec={"branch": "main", "spec": {}},
        expected_branch="main",
        description="Falls back to project_spec.branch"
    )

    run_test(
        "Test namespace with custom branch",
        project_config={"project": "my_project"},
        project_spec={"branch": "feature_branch", "spec": {"metaflow_branch": "test.feature_branch"}},
        expected_branch="test.feature_branch",
        description="Without [dev-assets], read/write same branch"
    )

    # Summary
    print("=" * 60)
    print(f"Results: {tests_passed} passed, {tests_failed} failed")
    print("=" * 60)

    if tests_failed == 0:
        print("\nAll tests PASSED!")
        return 0
    else:
        print("\nSome tests FAILED - check the resolve_scope implementation")
        return 1


def test_branch_sanitization():
    """Test branch name sanitization."""
    print()
    print("=" * 60)
    print("Testing branch name sanitization")
    print("=" * 60)
    print()

    from obproject.assets import _sanitize_branch_name

    test_cases = [
        ("test.data_model_reg", "test_data_model_reg"),
        ("user@company.com", "user_at_company_com"),
        ("feature/my-branch", "feature_my-branch"),
        ("UPPERCASE", "uppercase"),
        ("already_valid", "already_valid"),
    ]

    all_passed = True
    for raw, expected in test_cases:
        result = _sanitize_branch_name(raw)
        if result == expected:
            print(f"  [PASS] '{raw}' -> '{result}'")
        else:
            print(f"  [FAIL] '{raw}' -> '{result}' (expected '{expected}')")
            all_passed = False

    print()
    if all_passed:
        print("All sanitization tests passed!")
    else:
        print("Some sanitization tests failed!")

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit_code = test_resolve_scope()
    exit_code += test_branch_sanitization()
    sys.exit(exit_code)
