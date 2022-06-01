import setuptools

setuptools.setup(
    name="vuanem-caresoft-dataflow",
    version="0.1.0",
    install_requires=[
        "asyncio-throttle",
        "httpx",
        "google-cloud-secret-manager",
    ],
    packages=setuptools.find_packages(),
)
