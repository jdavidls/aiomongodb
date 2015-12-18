from setuptools import setup

setup(
    name="aiomongodb",							      version='0.1.0',
	author="JDavid Luque",			author_email="jdavidls@gmail.com",
    license="Apache License, Version 2.0",
    url="https://github.com/jdavidls/aiomongodb",
	description="""
		Python 3.5 asyncio mongodb driver
	""",
	keywords=[
		"mongo", "mongodb",	"asyncio"
	],
    install_requires=[
		'pymongo' # for bson
	],
    packages=["aiomongodb"],

    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database"
	],
)
