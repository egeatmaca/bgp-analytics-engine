from setuptools import setup, Extension
from Cython.Build import cythonize


extensions = [
    Extension(
        "update_loader",
        ["update_loader.pyx"],
        extra_compile_args=["-O3", "-march=native", "-fopenmp"],
        extra_link_args=["-fopenmp"],
    )
]

setup(
    ext_modules=cythonize(extensions, annotate=True, compiler_directives={
        'language_level': '3',
        'boundscheck': False,
        'wraparound': False,
        'initializedcheck': False,
        'cdivision': True,
    }),
    install_requires=[
        'pybgpstream'
    ],
    zip_safe=False,
)
 
