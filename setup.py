from setuptools import setup, find_namespace_packages

install_requires = [
    'sentry==25.*',
    'elasticsearch==8.*',
]

with open("README.md", "r") as readme:
    long_description = readme.read()

setup(
    name='sentry-nodestore-elastic',
    version='1.0.2',
    author='andrsp@gmail.com',
    author_email='andrsp@gmail.com',
    url='https://github.com/andrsp/sentry-nodestore-elastic',
    description='Sentry nodestore Elasticsearch backend',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_namespace_packages(),
    include_package_data=True,
    license='Apache-2.0',
    install_requires=install_requires,
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI App roved :: Apache Software License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
    ],
    project_urls={
        'Bug Tracker': 'https://github.com/andrsp/sentry-nodestore-elastic/issues',
        'CI': 'https://github.com/andrsp/sentry-nodestore-elastic/actions',
        'Source Code': 'https://github.com/andrsp/sentry-nodestore-elastic',
    },
    keywords=['sentry', 'elasticsearch', 'nodestore', 'backend'],
)
