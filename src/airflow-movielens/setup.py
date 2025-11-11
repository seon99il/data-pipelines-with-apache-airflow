import setuptools

requirements = ["apache-airflow==2.9.1", "requests"]


setuptools.setup(
    name="airflow-movielens",
    version="0.1.0",
    description="Hooks, sensors and operators for the Movielens API.",
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    install_requires=requirements,
    # url="https://github.com/movielens/airflow-movielens",
    license="MIT License",
)