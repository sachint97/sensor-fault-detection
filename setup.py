from setuptools import setup,find_packages
from typing import List

def get_requirements()->List[str]:
    """ This function returns list of requirements"""
    requirements_list:List[str] = []
    with open("requirements.txt","r") as requirements:
        for requirement in requirements.readlines():
            requirements_list.append(requirement.split("\n")[0])
        requirements_list.remove('-e .')
        print(requirements_list)
    return requirements_list


setup(
    name="sensor-fault-detection",
    version="0.0.1",
    author="sachin",
    author_email="cssachinshoba@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements()
)

