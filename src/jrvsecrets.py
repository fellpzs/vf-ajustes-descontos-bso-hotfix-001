import boto3
import os
import simplejson as json

_secrets = None

def _get_secrets(refresh=False):
    global _secrets
    if _secrets is None or refresh:
        sm = boto3.Session(region_name='us-east-1').client('secretsmanager')
        resp = sm.get_secret_value(
            SecretId=os.environ['SECRET_ID']
        )
        _secrets = json.loads(resp['SecretString'])
    return _secrets

def get_secret(key, safe=False):    
    if safe or key in _get_secrets():
        return _get_secrets().get(key)
    return _get_secrets(refresh=True)[key]