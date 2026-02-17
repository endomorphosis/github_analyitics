from __future__ import annotations

from pathlib import Path

from setuptools import find_packages, setup


def read_requirements() -> list[str]:
    req = Path(__file__).parent / "requirements.txt"
    if not req.exists():
        return []
    lines: list[str] = []
    for line in req.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


setup(
    name="github-analyitics",
    version="0.1.0",
    description="GitHub analytics + unified timestamp collection suite",
    long_description=(Path(__file__).parent / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    license_files=["LICENSE"],
    python_requires=">=3.10",
    packages=find_packages(include=["github_analyitics", "github_analyitics.*"]),
    include_package_data=True,
    install_requires=read_requirements(),
    entry_points={
        "console_scripts": [
            "github-analyitics-report=github_analyitics.reporting.github_analytics:main",
            "github-analyitics-timestamps=github_analyitics.timestamp_audit.timestamp_suite:main",
            "github-analyitics-timestamps-user=github_analyitics.timestamp_audit.timestamp_suite_user:main",
            "github-analyitics-local=github_analyitics.timestamp_audit.local_git_analytics:main",
            "github-analyitics-clone=github_analyitics.reporting.clone_and_analyze:main",
        ]
    },
)
