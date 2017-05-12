try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools

    use_setuptools()
    from setuptools import setup

setup(
    name='pgfire',
    version='0.1',
    description='Firebase like realtime database with postgres storage',
    author='Kapil Ratnani',
    author_email='kapil.ratnani@iiitb.net',
    license='GPL',
    url='http://github.com/kapilratnani/pgfire',
    download_url='http://github.com/kapilratnani/pgfire',
    packages=['pgfire'],
    package_dir={'pgfire': 'pgfire'}
)
