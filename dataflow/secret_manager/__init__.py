from google.cloud import secretmanager
from google.auth import default


def get_token():
    with secretmanager.SecretManagerServiceClient() as client:
        res = client.access_secret_version(
            request={"name": f"projects/{default()[1]}/secrets/caresoft/versions/1"}
        )
        return res.payload.data.decode("utf-8")
