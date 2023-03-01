from setuptools import setup, find_packages

setup(
    name="MatchmakingUsingFlink",
    version="0.0.1",
    description="Matchmaking Using Flink",
    author="sid-vibha",
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Matchmaking Using Flink",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "apache-flink==1.16.0"
    ],
    extras_require={
        "test": [
            "tox==3.27.0"
        ]
    }
)
