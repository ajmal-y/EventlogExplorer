import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="eventlogexplorer", # Replace with your own username
    version="0.1.0",
    author="Ajmal Yusuf",
    author_email="ajmal.yusuf@databricks.com",
    description="Spark Eventlog Explorer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    #packages=setuptools.find_packages(),
    packages=['eventlogexplorer'],
    package_dir={'eventlogexplorer': 'eventlogexplorer'},
    package_data={'eventlogexplorer': ['templates/*.j2', 'templates/*.html']},
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
    ####python_requires='>=3.5',
)

