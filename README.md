## gharchive-to-iceberg

Push data from gharchive.org to an Iceberg table in S3

```sh
cp .env.example .env # and fill the env variables
source .env
python -m venv env
source env/bin/activate
pip install -e .
python -m src.github_archive_to_iceberg --date 2011-02-12
```

