from setuptools import find_packages, setup


setup(
    name="london-data-model",
    version="0.1.0",
    description="GitHub-first local data pipelines for UK area exploration.",
    long_description="Project scaffold for local UK data pipelines.",
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=find_packages("src"),
    extras_require={
        "dev": [
            "pytest>=8.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "ldm=london_data_model.cli:main",
        ]
    },
)
