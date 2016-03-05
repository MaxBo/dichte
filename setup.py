# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
#from distutils.extension import Extension
#from cythonhelpers.make_cython_extensions import make_extensions


ext_modnames = []

setup(
    name="dichte_berlin",
    version="0.1",
    description="Dichteauswertungen Berlin und Vergleichsstädte",
    classifiers=[
      "Programming Language :: Python",
      "Environment :: Plugins",
      "Intended Audience :: System Administrators",
      "License :: Other/Proprietary License",
      "Natural Language :: English",
      "Operating System :: OS Independent",
      "Programming Language :: Python",
                ],
    keywords='density',
    download_url='',
    license='other',
    packages=find_packages('src', exclude=['ez_setup']),
    namespace_packages=[],

    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    data_files=[],
    install_requires=[
        'setuptools',
        ],
    #ext_modules=make_extensions(ext_modnames),
)

