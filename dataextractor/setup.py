
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
     name='dataextractor',
     version='0.3',
     scripts=['dataextractor.py'] ,
     author="Kacper Wozniak",
     author_email="kacper.wozniak@audiencenetwork.pl",
     description="Package to extract new varibles from Cloud Technologies data\
     stream",
     packages=setuptools.find_packages())
